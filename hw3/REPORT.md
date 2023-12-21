Use 1 late day, 3 left

build failed on gradescope, but passed locally

➜  build ctest -V -R btree_test_valgrind
UpdateCTestConfiguration  from :/mnt/c/Users/leque/Downloads/lab3.1/buzzdb-lab3_ish/build/DartConfiguration.tcl
UpdateCTestConfiguration  from :/mnt/c/Users/leque/Downloads/lab3.1/buzzdb-lab3_ish/build/DartConfiguration.tcl
Test project /mnt/c/Users/leque/Downloads/lab3.1/buzzdb-lab3_ish/build    
Constructing a list of tests
Done constructing a list of tests
Updating test list for fixtures
Added 0 tests to meet fixture requirements
Checking test dependency graph...
Checking test dependency graph end        
test 2
    Start 2: btree_test_valgrind

2: Test command: /usr/bin/valgrind "--error-exitcode=1" "--leak-check=full" "--soname-synonyms=somalloc=*jemalloc*" "--trace-children=yes" "--track-origins=yes" "--suppressions=/mnt/c/Users/leque/Downloads/lab3.1/buzzdb-lab3_ish/script/valgrind.supp" "/mnt/c/Users/leque/Downloads/lab3.1/buzzdb-lab3_ish/build/test/btree_test_valgrind" "--gtest_color=yes" "--gtest_output=xml:/mnt/c/Users/leque/Downloads/lab3.1/buzzdb-lab3_ish/build/test/unit_btree_test_valgrind.xml"
2: Test timeout computed to be: 10000000
2: ==17474== Memcheck, a memory error detector
2: ==17474== Copyright (C) 2002-2017, and GNU GPL'd, by Julian Seward et al.
2: ==17474== Using Valgrind-3.15.0 and LibVEX; rerun with -h for copyright info
2: ==17474== Command: /mnt/c/Users/leque/Downloads/lab3.1/buzzdb-lab3_ish/build/test/btree_test_valgrind --gtest_color=yes --gtest_output=xml:/mnt/c/Users/leque/Downloads/lab3.1/buzzdb-lab3_ish/build/test/unit_btree_test_valgrind.xml
2: ==17474==
2: [==========] Running 11 tests from 1 test case.
2: [----------] Global test environment set-up.
2: [----------] 11 tests from BTreeTest
2: [ RUN      ] BTreeTest.InsertEmptyTree
2: [       OK ] BTreeTest.InsertEmptyTree (23 ms)
2: [ RUN      ] BTreeTest.InsertLeafNode
2: [       OK ] BTreeTest.InsertLeafNode (9 ms)
2: [ RUN      ] BTreeTest.InsertLeafNodeSplit
2: [       OK ] BTreeTest.InsertLeafNodeSplit (30 ms)
2: [ RUN      ] BTreeTest.LookupEmptyTree
2: [       OK ] BTreeTest.LookupEmptyTree (11 ms)
2: [ RUN      ] BTreeTest.LookupSingleLeaf
2: [       OK ] BTreeTest.LookupSingleLeaf (11 ms)
2: [ RUN      ] BTreeTest.LookupSingleSplit
2: [       OK ] BTreeTest.LookupSingleSplit (10 ms)
2: [ RUN      ] BTreeTest.LookupMultipleSplitsIncreasing
2: [       OK ] BTreeTest.LookupMultipleSplitsIncreasing (128 ms)
2: [ RUN      ] BTreeTest.LookupMultipleSplitsDecreasing
2: [       OK ] BTreeTest.LookupMultipleSplitsDecreasing (32 ms)
2: [ RUN      ] BTreeTest.LookupRandomNonRepeating
2: [       OK ] BTreeTest.LookupRandomNonRepeating (30 ms)
2: [ RUN      ] BTreeTest.LookupRandomRepeating
2: [       OK ] BTreeTest.LookupRandomRepeating (23 ms)
2: [ RUN      ] BTreeTest.Erase
2: [       OK ] BTreeTest.Erase (17 ms)
2: [----------] 11 tests from BTreeTest (343 ms total)
2:
2: [----------] Global test environment tear-down
2: [==========] 11 tests from 1 test case ran. (405 ms total)
2: [  PASSED  ] 11 tests.
2: ==17474== 
2: ==17474== HEAP SUMMARY:
2: ==17474==     in use at exit: 0 bytes in 0 blocks
2: ==17474==   total heap usage: 31,509 allocs, 31,509 frees, 1,655,443 bytes allocated
2: ==17474==
2: ==17474== All heap blocks were freed -- no leaks are possible
2: ==17474==
2: ==17474== For lists of detected and suppressed errors, rerun with: -s  
2: ==17474== ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 0 from 0)
1/1 Test #2: btree_test_valgrind ..............   Passed    1.62 sec      

The following tests passed:
        btree_test_valgrind

100% tests passed, 0 tests failed out of 1

Total Test time (real) =   1.66 sec



