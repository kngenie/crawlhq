//
//

typedef char byte;
typedef unsigned long long int poly;

class FPGenerator {
  int degree;
  poly polynomial;
  poly empty;
  poly byteModTable[16][256];
public:
  FPGenerator(poly polynomial, int degree);
  poly fp(const char *buf, int start, int n);
  poly extend(poly f, const char *buf, int start, int n);
  // Return a value equal (mod polynomial) to fp and
  // of degree less than degree.
  poly reduce(poly fp);
  // Extends f with lower eight bits of v
  // without full reduction. In other words, returns a
  // polynomial that is equal (mod polynomial) to the
  // desired fingerprint but may be of higher degree than
  // the desired fingerprint
  poly extend_byte(poly f, byte v);
};
