/**
 * @file SqlParser.cpp
 * @author Vincent Marklynn, Yu Zhong (William)
 * 
 */ 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"

int main(int argc, char *argv[])
{
  if (argc != 2)
  {
    std::cerr << "Usage: cpsc5300: dbenvpath" << std::endl;
    return 1;
  }

  std::string envHome = argv[1];

  std::cout << envHome << std::endl;

}