use {
    crate::{
        producer::KafkaProducer,
        ledger_storage::LedgerStorage,
    },
    solana_binary_encoder::{
        transaction_status::{
            EncodedConfirmedBlock,
            UiTransactionEncoding,
            BlockEncodingOptions,
            TransactionDetails,
        },
        convert_block,
    },
    futures::StreamExt,
    log::{debug, error as log_error, info, warn},
    bytes::BytesMut,
    rdkafka::{
        config::{ClientConfig, RDKafkaLogLevel},
        consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer},
        message::{Message, BorrowedMessage},
    },
    std::{io::{self, BufRead, BufReader, Read}, time::{Duration, Instant}},
    std::str,
    std::fmt,
    flate2::read::GzDecoder,
    // hdfs::{HdfsFsCache, HdfsFs, HdfsFile, HdfsErr},
    serde_json::Value,
    anyhow::Result,
};
use hdfs::hdfs::{get_hdfs_by_full_path, HdfsFs, HdfsErr, HdfsFile};
use std::sync::Arc;

// pub struct HdfsFileReader<'a> {
//     hdfs_file: HdfsFile<'a>,
// }
//
// impl<'a> HdfsFileReader<'a> {
//     pub fn new(hdfs_file: HdfsFile<'a>) -> Self {
//         HdfsFileReader { hdfs_file }
//     }
// }
//
// impl<'a> Read for HdfsFileReader<'a> {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         self.hdfs_file.read(buf)
//             .map(|bytes_read| bytes_read as usize) // Convert i32 to usize
//             .map_err(|e| io::Error::new(io::ErrorKind::Other, MyError::Hdfs(e).to_string())) // Convert `HdfsErr` to `io::Error`
//     }
// }

pub struct HdfsFileReader {
    hdfs_file: HdfsFile,
}

impl HdfsFileReader {
    pub fn new(hdfs_file: HdfsFile) -> Self {
        HdfsFileReader { hdfs_file }
    }
}

impl Read for HdfsFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.hdfs_file.read(buf)
            .map(|bytes_read| bytes_read as usize) // Convert i32 to usize
            .map_err(|e| io::Error::new(io::ErrorKind::Other, MyError::Hdfs(e).to_string())) // Convert `HdfsErr` to `io::Error`
    }
}

// pub enum MyError {
//     #[error("I/O error: {0}")]
//     Io(#[from] io::Error),
//
//     #[error("HDFS error: {0}")]
//     Hdfs(HdfsErr),
//
//     #[error("JSON parsing error: {0}")]
//     Json(#[from] serde_json::Error),
// }
pub enum MyError {
    Io(io::Error),
    Hdfs(HdfsErr),
    Json(serde_json::Error),
}

impl fmt::Debug for MyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MyError::Io(e) => write!(f, "Io({:?})", e),
            MyError::Hdfs(e) => {
                // Provide specific messages for each variant of `HdfsErr`
                let hdfs_message = match e {
                    HdfsErr::Generic(_) => "Unknown HDFS error".to_string(),
                    HdfsErr::FileNotFound(path) => format!("File not found: {}", path),
                    HdfsErr::FileAlreadyExists(path) => format!("File already exists: {}", path),
                    HdfsErr::CannotConnectToNameNode(address) => format!("Cannot connect to NameNode at: {}", address),
                    HdfsErr::InvalidUrl(url) => format!("Invalid HDFS URL: {}", url),
                };
                write!(f, "Hdfs({})", hdfs_message)
            },
            MyError::Json(e) => write!(f, "Json({:?})", e),
        }
    }
}

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MyError::Io(e) => write!(f, "I/O error: {}", e),
            MyError::Hdfs(e) => {
                let hdfs_message = match e {
                    HdfsErr::Generic(_) => "Unknown HDFS error".to_string(),
                    HdfsErr::FileNotFound(path) => format!("File not found: {}", path),
                    HdfsErr::FileAlreadyExists(path) => format!("File already exists: {}", path),
                    HdfsErr::CannotConnectToNameNode(address) => format!("Cannot connect to NameNode at: {}", address),
                    HdfsErr::InvalidUrl(url) => format!("Invalid HDFS URL: {}", url),
                };
                write!(f, "HDFS error: {}", hdfs_message)
            },
            MyError::Json(e) => write!(f, "JSON parsing error: {}", e),
        }
    }
}

impl From<HdfsErr> for MyError {
    fn from(e: HdfsErr) -> Self {
        MyError::Hdfs(e)
    }
}

impl From<io::Error> for MyError {
    fn from(e: io::Error) -> Self {
        MyError::Io(e)
    }
}

impl From<serde_json::Error> for MyError {
    fn from(e: serde_json::Error) -> Self {
        MyError::Json(e)
    }
}

// pub struct KafkaConsumer<'a> {
pub struct KafkaConsumer {
    kafka_consumer: StreamConsumer,
    kafka_producer: KafkaProducer,
    storage: LedgerStorage,
    // hdfs_client: HdfsClient,
    // hdfs_cache: Rc<RefCell<HdfsFsCache<'a>>>,
    hdfs_namenode_url: String,
}

// impl<'a> KafkaConsumer<'a> {
impl KafkaConsumer {
    /// Create a new KafkaConsumer.
    pub async fn new(
        kafka_brokers: &str,
        group_id: &str,
        topics: &[&str],
        storage: LedgerStorage,
        kproducer: KafkaProducer,
        hdfs_namenode_url: &str, // HDFS NameNode URL
    // ) -> KafkaConsumer<'a> {
    ) -> KafkaConsumer {
        info!("Connecting to Kafka at {:?} with consumer group {:?}", kafka_brokers, group_id);
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", kafka_brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "10000")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("max.partition.fetch.bytes", "10485760")
            .set("max.in.flight.requests.per.connection", "1")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(topics)
            .expect("Failed to subscribe to specified topics");

        // Initialize the HDFS client using the hdfs-rs library
        // let hdfs_client = HdfsClient::connect(hdfs_namenode_url)
        //     .expect("Failed to create HDFS client");

        // let hdfs_cache = Rc::new(RefCell::new(HdfsFsCache::new()));

        KafkaConsumer {
            kafka_consumer: consumer,
            kafka_producer: kproducer,
            storage,
            // hdfs_cache,
            hdfs_namenode_url: hdfs_namenode_url.to_string(),
        }
    }

    /// Consume the incoming topic and process either direct JSON data or .gz files from HDFS.
    pub async fn consume(&self) {
        info!("Initiating consumption from Kafka topic");

        let mut message_counter = 0;
        let report_interval = 10;
        let mut batch_time = Instant::now();

        let mut message_stream = self.kafka_consumer.stream();
        while let Some(message) = message_stream.next().await {
            match message {
                Err(e) => warn!("Kafka error: {}", e),
                Ok(m) => {
                    if let Some(raw_data) = m.payload() {
                        let message_str = match str::from_utf8(raw_data) {
                            Ok(s) => s,
                            Err(e) => {
                                warn!("Failed to parse message as UTF-8: {}", e);
                                self.handle_error(&m, "Invalid UTF-8 message", e.to_string())
                                    .await;
                                continue;
                            }
                        };

                        // Parse the Kafka message as JSON
                        match serde_json::from_str::<Value>(message_str) {
                            Ok(parsed_json) => {
                                // Determine if the message contains an HDFS file path or direct block data
                                if let Some(file_path) =
                                    parsed_json.get("hdfs_path").and_then(|v| v.as_str())
                                {
                                    // HDFS file path scenario
                                    info!("Received HDFS file path: {}", file_path);

                                    if let Err(e) = self.process_hdfs_file(file_path).await {
                                        self.handle_error(&m, file_path, e.to_string()).await;
                                    }
                                } else {
                                    // Inline JSON block data scenario
                                    info!("Received inline block data");

                                    if let Err(e) =
                                        self.process_json_block(&parsed_json, message_str).await
                                    {
                                        self.handle_error(&m, message_str, e.to_string()).await;
                                    }
                                }
                            }
                            Err(_) => {
                                // Fallback to direct JSON string, if it's not a valid JSON object
                                warn!(
                                    "Failed to parse Kafka message as JSON, assuming raw block data"
                                );
                                if let Err(e) =
                                    self.process_json_block_direct(message_str).await
                                {
                                    self.handle_error(&m, message_str, e.to_string()).await;
                                }
                            }
                        }

                        // Commit the Kafka message after processing
                        self.commit_message(&m);
                    } else {
                        warn!("Failed to read payload from Kafka topic")
                    }
                }
            };

            message_counter += 1;

            if message_counter % report_interval == 0 {
                let batch_duration: Duration = batch_time.elapsed();
                info!(
                    "Processed {} messages, total time taken: {:?}",
                    report_interval, batch_duration
                );
                batch_time = Instant::now();
            }
        }

        debug!("Returned from consumer");
    }

    /// Process a .gz file from HDFS, decompress it, and parse newline-delimited JSON.
    async fn process_hdfs_file(&self, hdfs_path: &str) -> Result<(), MyError> {
        info!("Fetching and processing .gz file from HDFS: {}", hdfs_path);

        info!("Accessing hdfs instance from cache: {:?}", self.hdfs_namenode_url.as_str());

        // Access HDFS filesystem through the cache
        // let fs = self.hdfs_cache
        //     .borrow_mut()
        //     .get(self.hdfs_namenode_url.as_str())?; // `HdfsErr` is automatically converted to `MyError` here

        let fs: Arc<HdfsFs> = get_hdfs_by_full_path(self.hdfs_namenode_url.as_str()).ok().unwrap();

        info!("Accessing hdfs path: {:?}", hdfs_path.clone());

        // Read the .gz file from HDFS
        let hdfs_file = fs.open(hdfs_path)?; // `HdfsErr` is automatically converted to `MyError` here

        // Wrap `HdfsFile` in `HdfsFileReader` for `std::io::Read` compatibility
        let hdfs_file_reader = HdfsFileReader::new(hdfs_file);

        let decompressor = GzDecoder::new(hdfs_file_reader);
        let mut buffered_reader = BufReader::new(decompressor);

        let mut line = String::new();
        while buffered_reader.read_line(&mut line)? > 0 {
            let json_line = line.trim();

            // Process each JSON message
            let parsed_json: Value = serde_json::from_str(json_line)?; // `serde_json::Error` converted to `MyError`
            self.process_json_block(&parsed_json, json_line).await?;

            line.clear();
        }

        Ok(())
    }

    /// Process a block of JSON data directly from the Kafka message.
    async fn process_json_block(&self, parsed_json: &Value, json_data: &str) -> Result<(), io::Error> {
        // Extract the blockID from the JSON message
        let block_id = parsed_json["blockID"].as_u64().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Missing 'blockID' in JSON")
        })?;

        info!("Processing block with id {}", block_id);

        // Deserialize the full block data from JSON
        let block: EncodedConfirmedBlock = serde_json::from_str(json_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Invalid JSON block data: {}", e)))?;

        let options = BlockEncodingOptions {
            transaction_details: TransactionDetails::Full,
            show_rewards: true,
            max_supported_transaction_version: Some(0),
        };

        match convert_block(block, UiTransactionEncoding::Json, options) {
            Ok(versioned_block) => {
                self.storage
                    .upload_confirmed_block(block_id, versioned_block)
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            }
            Err(e) => {
                warn!("Failed to convert block: {}", e);
                return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
            }
        }

        Ok(())
    }

    /// Process a JSON block directly without parsing as JSON first.
    async fn process_json_block_direct(&self, json_data: &str) -> Result<(), io::Error> {
        let parsed_json: Value = serde_json::from_str(json_data)?;
        self.process_json_block(&parsed_json, json_data).await
    }

    async fn handle_error(&self, _m: &BorrowedMessage<'_>, data: &str, error_string: String) {
        warn!("Failed to process message {}: {}", data, error_string);
        let payload = BytesMut::from(data);
        self.kafka_producer.produce_with_headers(payload, None).await;
    }

    fn commit_message(&self, m: &BorrowedMessage) {
        if let Err(e) = self.kafka_consumer.commit_message(m, CommitMode::Async) {
            log_error!("Failed to commit offset to Kafka: {:?}", e);
        }
    }
}
