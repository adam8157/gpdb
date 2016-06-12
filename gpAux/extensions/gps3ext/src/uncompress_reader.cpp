#include "uncompress_reader.h"
#include "s3log.h"
#include "s3macros.h"

UncompressReader::UncompressReader() {
    this->reader = NULL;
    this->in = new char[S3_ZIP_CHUNKSIZE];
    this->out = new char[S3_ZIP_CHUNKSIZE];
}

UncompressReader::~UncompressReader() {
    delete this->in;
    delete this->out;
}

// Used for unit test to adjust buf size
void UncompressReader::resizeUncompressReaderBuffer(int size) {
    delete this->in;
    delete this->out;
    this->in = new char[size];
    this->out = new char[size];
}

void UncompressReader::setReader(Reader *reader) {
    this->reader = reader;
}

void UncompressReader::open(const ReaderParams &params) {
    // allocate inflate state
    zstream.zalloc = Z_NULL;
    zstream.zfree = Z_NULL;
    zstream.opaque = Z_NULL;
    zstream.next_in = Z_NULL;
    zstream.next_out = (Byte *)this->out;

    zstream.avail_in = 0;
    zstream.avail_out = S3_ZIP_CHUNKSIZE;
    this->offset = 0;

    // 47 is the number of windows bits, to make sure zlib could recognize and decode gzip stream.
    int ret = inflateInit2(&zstream, 47);
    CHECK_OR_DIE_MSG(ret == Z_OK, "%s", "failed to initialize zlib library");
}

uint64_t UncompressReader::read(char *buf, uint64_t count) {
    // 1. get data from out if out has data.

    // 2. fill out buffer
    int has = S3_ZIP_CHUNKSIZE - this->zstream.avail_out;

    if (this->offset < has) {
        // read remaining data in out buffer uncompressed last time.
    } else {
        this->uncompress();
        has = S3_ZIP_CHUNKSIZE - this->zstream.avail_out;
    }

    //    printf("has = %d, count = %d\n", has, count);

    int ret = (has > count ? count : has);
    memcpy(buf, this->out, ret);

    // offset?...
    this->offset += ret;

    return ret;
}

// Read compressed data from underlying reader and uncompress to this->out buffer.
// If no more data to consume, this->zstream.avail_out == S3_ZIP_CHUNKSIZE;
void UncompressReader::uncompress() {
    this->offset = 0;  // reset cursor for out buffer to read from beginning.

    if (this->zstream.avail_in == 0) {
        this->zstream.avail_out = S3_ZIP_CHUNKSIZE;
        this->zstream.next_out = (Byte *)this->out;

        // 1. reader S3_ZIP_CHUNKSIZE data from underlying reader and put into this->in buffer.
        uint64_t hasRead = this->reader->read(this->in, S3_ZIP_CHUNKSIZE);
        if (hasRead == 0) {
            S3DEBUG(
                "No more data to uncompress: avail_in = %d, avail_out = %d, total_in = %d, "
                "total_out = %d\n",
                zstream.avail_in, zstream.avail_out, zstream.total_in, zstream.total_out);
            return;  // EOF
        }

        this->zstream.next_in = (Byte *)this->in;
        this->zstream.avail_in = hasRead;
    } else {
        // Still have more data in 'in' buffer to decode.
        this->zstream.avail_out = S3_ZIP_CHUNKSIZE;
        this->zstream.next_out = (Byte *)this->out;
    }

    S3DEBUG("Before decompress: avail_in = %d, avail_out = %d, total_in = %d, total_out = %d\n",
            zstream.avail_in, zstream.avail_out, zstream.total_in, zstream.total_out);

    int status = inflate(&this->zstream, Z_NO_FLUSH);
    switch (status) {
        case Z_STREAM_END:
            S3DEBUG("compression finished: Z_STREAM_END.");
            break;
        case Z_STREAM_ERROR:
        case Z_NEED_DICT:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            inflateEnd(&this->zstream);
            CHECK_OR_DIE_MSG(false, "failed to decompress data: %d", status);
    }

    S3DEBUG("After decompress: avail_in = %d, avail_out = %d, total_in = %d, total_out = %d\n",
            zstream.avail_in, zstream.avail_out, zstream.total_in, zstream.total_out);

    return;
}

void UncompressReader::close() {
    inflateEnd(&zstream);
}