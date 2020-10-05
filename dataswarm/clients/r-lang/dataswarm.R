#
# quick test: R CMD BATCH dataswarm.R
#

#needed packages:
#install.packages("R6")
#install.packages("rjson")
#install.packages("pack")

library("R6")
library("rjson")
library("pack")

DataSwarm <- R6Class("DataSwarm",
                     public = list(
                                   host = "localhost",
                                   port = 0,

                                   initialize = function(host = "localhost", port = "1234") {
                                       self$host <- host
                                       self$port <- port
                                       private$socket <- socketConnection(host=host, port=port, open="w+b", server=FALSE)
                                       private$handshake()
                                   },

                                   disconnect = function() {
                                       close(private$socket)
                                   },

                                   send_recv = function(msg) {
                                       private$id = private$id + 1
                                       msg$id = private$id
                                       self$send_str(toJSON(msg))
                                       return(self$recv())
                                   },

                                   send_str = function(msg) {
                                       header <- private$pack_header(nchar(msg))
                                       writeBin(header, private$socket, endian = "big") #using big-endian for network order
                                       write(msg, private$socket)
                                   },

                                   recv = function() {
                                       header <- private$recv_raw(8)
                                       size <- rawToNum(rev(header[5:8]))
                                       contents = fromJSON(rawToChar(private$recv_raw(size)))
                                       print(contents)
                                   }
                                   ),

                     private = list(
                                    id = 0,
                                    socket = NA,
                                    header_size = 8,
                                    header_init = c(charToRaw("M"), 0x51, 0x03, 0x00),   # M Q 0x03 0x00

                                    pack_header = function(size) {
                                        header <- c(as.raw(c(charToRaw("M"), charToRaw("Q"), 3, 0)), rev(numToRaw(size, 4)))
                                        return(header)
                                    },

                                    recv_raw = function(size) {
                                        contents = raw(size)
                                        total_read = 0
                                        while(total_read < size) {
                                            socketSelect(list(private$socket))
                                            read = readBin(private$socket, raw(), size - total_read)
                                            if(length(read) > 0) {
                                                contents[total_read+1:length(read)] = read
                                                total_read = total_read + length(read)
                                            }
                                        }
                                        return(contents)
                                    },

                                    handshake = function() {
                                        msg = list("method" = "handshake", "params" = list("type" = "client"))
                                        self$send_recv(msg)
                                    }
                                    ))


# Rscript dataswarm.R
ds <- DataSwarm$new(port=1234)

Sys.sleep(5)

ds$disconnect()

