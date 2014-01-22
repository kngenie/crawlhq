#define _LARGEFILE64_SOURCE
#include <iostream>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

using namespace std;

// KEY is signed - this makes it easier for python to handle it.
typedef long long KEY;
// time_t is now 64bit. this is a huge waste of space as upper 32bit will
// be zero until 2106/3/7.
//typedef time_t TS;
// this works until year 2106 (while 32bit time_t = int works until 2038).
typedef unsigned int TS;

#pragma pack(push, 4)
struct RECORD {
  KEY key;
  TS ts;
  void operator=(RECORD &other) {
    key = other.key;
    ts = other.ts;
  }
  bool operator<(RECORD &other) {
    return key < other.key || key == other.key && ts > other.ts;
  }
  bool operator==(RECORD &other) {
    return key == other.key && ts == other.ts;
  }
  static int compare(RECORD &a, RECORD &b) {
    if (a.key < b.key)
      return -1;
    if (a.key > b.key)
      return 1;
    // larger timestamp is considered smaller, so that later record
    // prevail during the merge (merge code relies on this behavior)
    // this also guarantees later record survives if there are multiple
    // reocrds of the same key within a SEEN file.
    if (a.ts > b.ts)
      return -1;
    if (a.ts < b.ts)
      return 1;
    return 0;
  }
};
#pragma pack(pop)

class MergeSource {
  int fd;
  off64_t start;
  off64_t bound;
  RECORD top;
public:
  MergeSource(const char *fn, off64_t start, off64_t bound)
    : start(start), bound(bound) {
    assert(bound > start);
    this->fd = open(fn, O_RDONLY);
    if (fd < 0) {
      cerr << "failed to open " << fn << " (" << strerror(errno) << ")" << endl;
      exit(1);
    }
    off64_t pos = lseek64(fd, start, SEEK_SET);
    if (pos == (off64_t)-1) {
      cerr << "failed to seek to " << start << endl;
      exit(1);
    }
    fetch();
  }
  bool fetch() {
    if (fd < 0) {
      cerr << "fetch called on closed MergeSource" << endl;
      return false;
    }
    off64_t pos = lseek64(fd, 0, SEEK_CUR);
    if (pos >= bound) {
      ::close(fd);
      fd = -1;
      return false;
    }
    ssize_t bytes = read(fd, &top, sizeof(top));
    if (bytes != sizeof(top)) {
      ::close(fd);
      fd = -1;
      return false;
    }
    return true;
  }
  bool active() {
    return fd != -1;
  }
  bool operator<(MergeSource &other) {
    if (!active()) return false;
    if (!other.active()) return true;
    return top < other.top;
  }
  /*
  bool operator==(RECORD &other) {
    if (!active() || !other.active()) return false;
    return top == other;
  }
  */
  /*
  bool operator!=(RECORD &other) {
    if (!active() || !other.active()) return true;
    return !(top.key == other.top.key);
  }
  */
  void close() {
    fd = -1;
  }
  KEY copyrecord(int outfd) {
    if (!active()) return 0;
    KEY top_save = top.key;
    ssize_t n = write(outfd, &top, sizeof(top));
    assert(n == sizeof(top));

    fetch();
    return top_save;
  }
  void skip() {
    if (!active()) return;
    fetch();
  }
  bool samekey(KEY key) {
    return active() && top.key == key;
  }
};

void usage_and_exit() {
  cerr << "Usage: mseenrepair SEEN [SEEN...]" << endl
       << "uses SEEN.work for storing intermediate data. existing SEEN.work "
       << "will be silently truncated." << endl
       << "merged and sorted SEEN file will be saved in SEEN.fixed." << endl
       << "again, program will fail if file exists." << endl
       << "input SEEN files will not be modified." << endl;
  cerr << "record size is " << sizeof(RECORD) << endl;
  cerr << "  KEY:" << sizeof(KEY) << endl;
  cerr << "  TS:" << sizeof(TS) << endl;
  exit(1);
}

option options[] = {
  { "recordcount", 1, NULL, 'r' },
};

// default is 2GB worth of records
int maxRecordCount = 268435456;
mode_t seenfileMode = (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

int compareRecords(const void *a, const void *b) {
  return RECORD::compare(*(RECORD *)a, *(RECORD *)b);
}
  
// old code
int compareInt64(const void *a, const void *b) {
  KEY va = *(KEY *)a;
  KEY vb = *(KEY *)b;
  return va < vb ? -1 : (va > vb ? 1 : 0);
}

int main(int argc, char *argv[]) {
  const char *optstring = "";
  char optc;
  while (optc = getopt_long(argc, argv, optstring, options, NULL) != -1) {
    switch (optc) {
    case 'r':
      maxRecordCount = atoi(optarg);
      break;
    case '?':
      cerr << "undefined option" << endl;
      exit(1);
    }
  }
  
  if (optind >= argc) {
    usage_and_exit();
  }

  const char *workfile = "SEEN.work";
  const char *outfile = "SEEN.fixed";

  int workfd = open(workfile, O_WRONLY|O_CREAT|O_TRUNC, seenfileMode);
  if (workfd < 0) {
    cerr << "failed to open " << workfile << " for writing: "
	 << strerror(errno) << endl;
    exit(1);
  }
  for (int i = optind; i < argc; i++) {
    const char *seenfile = argv[i];
    cerr << "copying " << seenfile << " to workfile" << endl;
    int infd = open(seenfile, O_RDONLY);
    assert(infd >= 0);
    char buffer[512 * sizeof(KEY)];
    ssize_t bytes;
    while ((bytes = read(infd, buffer, sizeof(buffer))) > 0) {
      if (bytes < sizeof(buffer)) {
	bytes = (bytes / sizeof(KEY)) * sizeof(KEY);
      }
      ssize_t written = write(workfd, buffer, bytes);
      assert(written == bytes);
    }
    close(infd);
  }
  off64_t fileLen = lseek64(workfd, 0, SEEK_CUR);
  close(workfd);

  cerr << "blockwise sorting " << workfile << endl;
  workfd = open(workfile, O_RDWR);
  if (workfd < 0) {
    cerr << "failed to open " << workfile << endl;
    exit(1);
  }

  size_t recordSize = sizeof(KEY);
  assert(recordSize == 8);

  // 1. sort blockwise
  int blocks = 0;
  off64_t blockStart = 0;
  while (blockStart < fileLen) {
    size_t blockLen = maxRecordCount * recordSize;
    if (blockStart + blockLen > fileLen) {
      blockLen = fileLen - blockStart;
    }
    cerr << "sorting block " << blocks << " offset " << blockStart << endl;
    void *map;
    map = mmap(NULL, blockLen, PROT_READ|PROT_WRITE, MAP_SHARED,
	       workfd, blockStart);
    if (map == MAP_FAILED) {
      cerr << "failed to mmap " << blockStart << "[" << blockLen << "] of "
	   << workfile << ": " << strerror(errno) << endl;
      exit(1);
    }

    qsort(map, blockLen / recordSize, recordSize, compareInt64);

    munmap(map, blockLen);

    blockStart += blockLen;
    blocks++;
  }
  close(workfd);

  // 2. merge sorted blocks.
  cerr << "merging " << blocks << " blocks" << endl;
  MergeSource **sources = new MergeSource *[blocks];
  
  blockStart = 0;
  for (int i = 0; i < blocks; i++) {
    off64_t bound = blockStart + (maxRecordCount * recordSize);
    sources[i] = new MergeSource(workfile, blockStart, bound);
    blockStart = bound;
  }

  for (int i = 0; i < blocks - 1; i++) {
    for (int j = i + 1; j < blocks; j++) {
      if (*sources[j] < *sources[i]) {
	MergeSource *t = sources[j];
	sources[j] = sources[i];
	sources[i] = t;
      }
    }
  }
  
  int outfd = open(outfile, O_WRONLY|O_CREAT|O_EXCL, seenfileMode);
  if (outfd < 0) {
    cerr << "failed to open " << outfile << " for writing" << endl;
    exit(1);
  }
  
  while (sources[0]->active()) {
    KEY key = sources[0]->copyrecord(outfd);
    // move sources[0] to right position in the list.
    // skip and repeat while top has the same key (remove duplicates)
    for (;;) {
      for (int i = 0; i < blocks - 1; i++) {
	if (*sources[i] < *sources[i + 1]) break;
	MergeSource *t = sources[i];
	sources[i] = sources[i + 1];
	sources[i + 1] = t;
      }
      if (!sources[0]->samekey(key)) break;
      sources[0]->skip();
    }
  }

  close(outfd);
  for (int i = 0; i < blocks; i++) {
    if (sources[i]->active()) {
      cerr << "weird, sources[" << i << "] is still active" << endl;
      sources[i]->close();
    }
    delete sources[i];
  }
  delete[] sources;

  int s = unlink(workfile);
  if (s != 0) {
    cerr << "failed to delete workfile " << workfile << ": "
	 << strerror(errno) << endl;
  }
}

    
  
				 
