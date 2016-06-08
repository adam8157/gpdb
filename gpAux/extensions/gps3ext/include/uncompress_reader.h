/*-------------------------------------------------------------------------
 *
 * uncompress_reader.h
 *	uncompress reader read compressed data and uncompress it.
 *
 * Greenplum Database
 * Copyright (C) 2016 Pivotal Software, Inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef INCLUDE_UNCOMPRESS_READER_H_
#define INCLUDE_UNCOMPRESS_READER_H_

class UncompressReader : public Reader {
   public:
    UncompressReader();
    virtual ~UncompressReader();

    void open(const ReaderParams &params);

    // read() attempts to read up to count bytes into the buffer starting at
    // buffer.
    // Return 0 if EOF. Throw exception if encounters errors.
    uint64_t read(char *buf, uint64_t count);

    // This should be reentrant, has no side effects when called multiple times.
    void close();
};

#endif /* INCLUDE_UNCOMPRESS_READER_H_ */
