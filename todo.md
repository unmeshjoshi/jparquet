# Project Development Checklist

## Phase 1: Project Initialization

- [x] **1.1 Set Up Version Control**
  - [x] Initialize a Git repository for the project.
  - [x] Create a `.gitignore` file configured for Java projects to exclude build directories, dependency jars, and IDE-specific files.

- [x] **1.2 Configure Build Automation**
  - [x] Set up Gradle as the build automation tool.
  - [x] Create a `build.gradle` file specifying:
    - [x] Project metadata.
    - [x] Java 17 compatibility.
    - [x] Dependencies for JUnit 5 (testing) and Apache Parquet (Parquet file handling).

- [x] **1.3 Establish Project Structure**
  - [x] Create the standard Gradle project directory layout:
    - [x] `src/main/java`: Source code.
    - [x] `src/test/java`: Test code.
  - [x] Ensure package naming follows Java conventions (e.g., `com.jparque`).

- [x] **1.4 Integrate Testing Framework**
  - [x] Add JUnit 5 to the project dependencies in `build.gradle`.
  - [x] Create a sample test class in `src/test/java` to verify the testing setup.

## Phase 2: Core Module Development

- [x] **2.1 Define Schema Representation**
  - [x] Develop a `Schema` class to represent dataset schemas:
    - [x] Store column names and their data types.
    - [x] Provide methods to add new columns and retrieve data types by column name.
    - [x] Include validation to ensure data records conform to the schema.
  - [x] Write unit tests to verify the correctness of these methods and validations.

- [x] **2.2 Implement Data Serialization**
  - [x] Create a `ParquetSerializer` class responsible for converting Java objects into Parquet format:
    - [x] Accept data records as `Map<String, Object>`.
    - [x] Utilize the Apache Parquet library to write these records to a Parquet file.
    - [x] Ensure data adheres to the defined `Schema`.
  - [x] Develop unit tests to validate the serialization process, including schema adherence and correct data writing.

- [x] **2.3 Implement Data Deserialization**
  - [x] Develop a `ParquetDeserializer` class to read data from Parquet files and reconstruct them into Java objects:
    - [x] Open and read Parquet files using the Apache Parquet library.
    - [x] Convert data into a list of `Map<String, Object>` records.
    - [x] Ensure deserialized data aligns with the defined `Schema`.
  - [x] Write unit tests to verify accurate deserialization, schema compliance, and proper handling of various data types.

- [x] **2.4 Integrate Compression Algorithms**
  - [x] Enhance the `ParquetSerializer` class to support data compression:
    - [x] Implement functionality allowing users to specify a compression algorithm (e.g., Snappy, Gzip, Zstd) when writing data.
    - [x] Ensure the chosen compression is applied during serialization and correctly stored in the file metadata.
  - [x] Develop unit tests to confirm that data is compressed as specified and can be accurately decompressed during deserialization.

## Phase 3: File I/O Module

- [x] **3.1 Develop File Writer Class**
  - [x] Create a `ParquetFileWriter` class responsible for writing serialized data to Parquet files:
    - [x] Accept a list of data records (`List<Map<String, Object>>`) and a target file path.
    - [x] Use the `ParquetSerializer` to convert records into Parquet format.
    - [x] Write serialized data to the specified file location, ensuring proper file structure and integrity.
  - [x] Implement unit tests to verify successful file writing, correct data storage, and proper handling of I/O exceptions.

- [x] **3.2 Develop File Reader Class**
  - [x] Develop a `ParquetFileReader` class to read data from Parquet files:
    - [x] Accept a file path to a Parquet file.
    - [x] Use the `ParquetDeserializer` to read and convert the file's data into a list of `Map<String, Object>` records.
    - [x] Extract and interpret schema and compression metadata from the file's footer to ensure accurate data reconstruction.
  - [x] Write unit tests to confirm correct data reading, proper interpretation of metadata, and handling of various file structures.

## Phase 4: Metadata Module

- [ ] **4.1 Implement Schema Evolution Support**
  - [ ] Enhance the system to handle schema evolution, allowing for changes in data schema over time:
    - [x] Implement mechanisms to track schema versions.
    - [ ] Manage backward and forward compatibility.
    - [ ] Apply transformations to adapt data to the current schema version.
  - [ ] Develop unit tests to verify that schema changes are managed correctly and that data remains consistent across schema versions.

- [ ] **4.2 Implement Transaction Log for ACID Operations**
  - [ ] Create a transaction log system to support ACID (Atomicity, Consistency, Isolation, Durability) properties in data operations:
    - [ ] Record all data modification transactions.
    - [ ] Support rollback and recovery mechanisms.
    - [ ] Ensure data integrity during concurrent operations.
  - [ ] Write unit tests to validate transaction logging, rollback functionality, and data consistency under various scenarios.

## Phase 5: Advanced Features

- [ ] **5.1 Implement Time Travel Functionality**
  - [ ] Enable querying of historical data states to facilitate data versioning and auditing:
    - [ ] Implement functionality to access data as of a specific timestamp or version.
    - [ ] Maintain a history of data changes.
  - [ ] Develop unit tests to ensure accurate retrieval of historical data and proper maintenance of data version history.

- [ ] **5.2 Develop Data Filtering Capabilities**
  - [ ] Add support for data filtering to retrieve specific subsets of data based on query conditions:
    - [ ] Implement functionality to apply predicate pushdown to optimize query performance.
    - [ ] Support various filter conditions (e.g., ranges, specific values).
  - [ ] Write unit tests to confirm that filtering works correctly and efficiently under different scenarios.

## Phase 6: Documentation and Educational Materials

- [ ] **6.1 Create Comprehensive Documentation**
  - [ ] Develop detailed documentation covering:
    - [ ] System architecture and design decisions.
    - [ ] Usage instructions for each component.
    - [ ] Examples demonstrating common use cases.
  - [ ] Ensure the documentation is clear and accessible to users with varying levels of expertise.

- [ ] **6.2 Develop Educational Examples**
  - [ ] Create a series of educational examples and tutorials to demonstrate system features:
    - [ ] Include sample code illustrating typical workflows.
    - [ ] Provide step-by-step guides for setting up and using the system.
    - [ ] Explain key concepts and components.
  - [ ] Ensure examples are well-documented and serve as practical learning resources.
