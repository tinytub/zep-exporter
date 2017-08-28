#include <string>
#include <iostream>
#include <functional>
#include <stdint.h>

extern "C" uint64_t chash(const char*);
uint64_t chash(const char *str) {
  return std::hash<std::string>()(std::string(str));
}

/*g++ -o chash chash.cc -std=c++11
int main() {
   const int size = 20;
   char key[size];
   std::cout<<"Enter hash string:";
   std::cin>>key;
   std::cout << chash(key) << std::endl;
}
*/