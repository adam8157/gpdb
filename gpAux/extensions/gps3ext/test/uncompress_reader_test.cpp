#include <vector>

#include "gtest/gtest.h"
#include "uncompress_reader.cpp"
#include "uncompress_reader.h"

using std::vector;

#define CHECK_ERR(err, msg)                              \
    {                                                    \
        if (err != Z_OK) {                               \
            fprintf(stderr, "%s error: %d\n", msg, err); \
            exit(1);                                     \
        }                                                \
    }

class BufferReader : public Reader {
   public:
    BufferReader() {
        this->offset = 0;
    }

    void open(const ReaderParams &params) {
    }
    void close() {
    }

    void setData(unsigned char *input, uint64_t size) {
        this->clear();
        this->data.insert(this->data.end(), input, input + size);
    }

    uint64_t read(char *buf, uint64_t count) {
        uint64_t remaining = this->data.size() - offset;
        if (remaining <= 0) {
            return 0;
        }

        uint64_t size = (remaining > count) ? count : remaining;

        for (int i = 0; i < size; i++) {
            buf[i] = this->data[offset + i];
        }

        this->offset += size;
        return size;
    }

    void clear() {
        this->data.clear();
        this->offset = 0;
    }

   private:
    std::vector<uint8_t> data;
    uint64_t offset;
};

class UncompressReaderTest : public testing::Test {
   protected:
    // Remember that SetUp() is run immediately before a test starts.
    virtual void SetUp() {
        uncompressReader.open(params);
        uncompressReader.setReader(&bufReader);
    }

    // TearDown() is invoked immediately after a test finishes.
    virtual void TearDown() {
        uncompressReader.close();
    }

    void setBufReaderByRawData(const char *input) {
        uLong len = (uLong)strlen(input) + 1;
        uLong compressedLen = sizeof(compressionBuff);

        int err = compress(compressionBuff, &compressedLen, (const Bytef *)input, len);
        CHECK_ERR(err, "failed to compress sample data");

        bufReader.setData(compressionBuff, compressedLen);
    }

    UncompressReader uncompressReader;
    ReaderParams params;
    BufferReader bufReader;
    Byte compressionBuff[10000];
};

TEST_F(UncompressReaderTest, AbleToUncompressEmptyData) {
    unsigned char input[10] = {0};
    BufferReader bufReader;
    bufReader.setData(input, 0);
    uncompressReader.setReader(&bufReader);

    char buf[10000];
    uint64_t count = uncompressReader.read(buf, sizeof(buf));

    EXPECT_EQ(0, count);
}

TEST_F(UncompressReaderTest, AbleToUncompressSmallCompressedData) {
    // 1. compressed data to uncompress
    const char hello[] = "Go IPO, Pivotal! Go Go Go!!!";
    setBufReaderByRawData(hello);

    // 2. call API
    char buf[10000];
    uint64_t count = uncompressReader.read(buf, sizeof(buf));

    // 3. assertion
    EXPECT_EQ(sizeof(hello), count);
    EXPECT_EQ(0, strncmp(hello, buf, count));
}

TEST_F(UncompressReaderTest, AbleToUncompressBigCompressedData) {
    // 1. compressed data to uncompress
    S3_ZIP_CHUNKSIZE = 32;
    uncompressReader.resizeUncompressReaderBuffer(S3_ZIP_CHUNKSIZE);

    char hello[S3_ZIP_CHUNKSIZE * 2 + 1];
    memset((void *)hello, 'A', sizeof(hello));
    hello[sizeof(hello) - 1] = '\0';

    setBufReaderByRawData(hello);

    char outputBuffer[16];

    // 3. first round: 16 bytes
    uint64_t count = uncompressReader.read(outputBuffer, sizeof(outputBuffer));
    EXPECT_EQ(sizeof(outputBuffer), count);

    // 4. 2nd round: 16 bytes
    count = uncompressReader.read(outputBuffer, sizeof(outputBuffer));
    EXPECT_EQ(sizeof(outputBuffer), count);
}