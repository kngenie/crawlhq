//
//

typedef char byte;
typedef unsigned long long int ulong;

class FPGenerator {
  int degree;
  ulong polynomial;
  ulong empty;
  ulong byteModTable[16][256];
public:
  FPGenerator(ulong polynomial, int degree);
  ulong fp(const char *buf, int start, int n);
  ulong extend(ulong f, const char *buf, int start, int n);
  // Return a value equal (mod polynomial) to fp and
  // of degree less than degree.
  ulong reduce(ulong fp);
  // Extends f with lower eight bits of v
  // without full reduction. In other words, returns a
  // polynomial that is equal (mod polynomial) to the
  // desired fingerprint but may be of higher degree than
  // the desired fingerprint
  ulong extend_byte(ulong f, byte v);
};
