#include <openssl/crypto.h>
#include <pthread.h>
#include <sstream>
#include <string>

#include "gpcommon.h"
#include "gpreader.h"
#include "s3conf.h"
#include "s3log.h"
#include "s3macros.h"
#include "s3utils.h"

// Thread related functions, called only by gpreader and gpcheckcloud
#define MUTEX_TYPE pthread_mutex_t
#define MUTEX_SETUP(x) pthread_mutex_init(&(x), NULL)
#define MUTEX_CLEANUP(x) pthread_mutex_destroy(&(x))
#define MUTEX_LOCK(x) pthread_mutex_lock(&(x))
#define MUTEX_UNLOCK(x) pthread_mutex_unlock(&(x))
#define THREAD_ID pthread_self()

/* This array will store all of the mutexes available to OpenSSL. */
static MUTEX_TYPE *mutex_buf = NULL;

static void locking_function(int mode, int n, const char *file, int line) {
    if (mode & CRYPTO_LOCK) {
        MUTEX_LOCK(mutex_buf[n]);
    } else {
        MUTEX_UNLOCK(mutex_buf[n]);
    }
}

static unsigned long id_function(void) {
    return ((unsigned long)THREAD_ID);
}

int thread_setup(void) {
    mutex_buf = new pthread_mutex_t[CRYPTO_num_locks()];
    if (mutex_buf == NULL) {
        return 0;
    }
    for (int i = 0; i < CRYPTO_num_locks(); i++) {
        MUTEX_SETUP(mutex_buf[i]);
    }
    CRYPTO_set_id_callback(id_function);
    CRYPTO_set_locking_callback(locking_function);
    return 1;
}

int thread_cleanup(void) {
    if (mutex_buf == NULL) {
        return 0;
    }
    CRYPTO_set_id_callback(NULL);
    CRYPTO_set_locking_callback(NULL);
    for (int i = 0; i < CRYPTO_num_locks(); i++) {
        MUTEX_CLEANUP(mutex_buf[i]);
    }
    delete mutex_buf;
    mutex_buf = NULL;
    return 1;
}

GPReader::GPReader(const string &urlWithOptions) {
    constructReaderParam(urlWithOptions);
    restfulServicePtr = &restfulService;
}

void GPReader::constructReaderParam(const string &urlWithOptions) {
    string config_path = get_opt_s3(urlWithOptions, "config");
    InitConfig(config_path.c_str(), "default");

    string url = truncate_options(urlWithOptions);
    params.setUrlToLoad(url);
    params.setSegId(s3ext_segid);
    params.setSegNum(s3ext_segnum);
    params.setNumOfChunks(s3ext_threadnum);
    params.setChunkSize(s3ext_chunksize);

    cred.accessID = s3ext_accessid;
    cred.secret = s3ext_secret;
    params.setCred(cred);
}

void GPReader::open(const ReaderParams &params) {
    s3service.setRESTfulService(restfulServicePtr);
    bucketReader.setS3interface(&s3service);
    keyReader.setS3interface(&s3service);
    bucketReader.setUpstreamReader(&keyReader);
    bucketReader.open(this->params);
}

// read() attempts to read up to count bytes into the buffer starting at
// buffer.
// Return 0 if EOF. Throw exception if encounters errors.
uint64_t GPReader::read(char *buf, uint64_t count) {
    return bucketReader.read(buf, count);
}

// This should be reentrant, has no side effects when called multiple times.
void GPReader::close() {
    bucketReader.close();
}
