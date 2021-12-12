cmake_minimum_required(VERSION 3.5)

project(historicalSqlite3)

include_directories(open62541)
include_directories(sqlite)

set(source main/server.c open62541/open62541.c sqlite/sqlite3.c)

add_executable(server  ${source})

target_link_libraries (server pthread dl)

add_executable(sqliteShell sqlite/shell.c sqlite/sqlite3.c)
target_link_libraries (sqliteShell pthread dl)


