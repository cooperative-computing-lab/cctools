require_relative '../rmv'

require 'optparse'
require 'pathname'

module RMV
  class Options

    def initialize argv
      @config = {source: nil,
                 debug: false,
                 destination: nil,
                 name: "",
                 overwrite: false,
                 workspace: Pathname.new("/tmp/rmv")}
      parse argv
      mandatory = [:source, :destination]
      missing = mandatory.select { |param| config[param].nil? }
      unless missing.empty?
        STDERR.puts "Missing options: #{missing.join(', ')}"
        exit -1
      end
    end

    def method_missing m, *a, &b
      @config.fetch(m){ super }
    end

    def parse argv
      OptionParser.new do |opts|
        opts.banner = "Usage:    rmv [options] "
        opts.on("-D", "--debug", "Keep intermediate files and produce more verbose output") do
          config[:debug] = true
        end
        opts.on("-d", "--destination path", String, "Directory in which to place the output") do |d|
          config[:destination] = Pathname.new(d).expand_path
        end
        opts.on("-h", "--help", "Show this message") do
          puts opts
          exit
        end
        opts.on("-n", "--name name", String, "Set the name of the workflow") do |n|
          config[:name] = n
        end
        opts.on("-o", "--overwrite", String, "Overwrite existing output files") { config[:overwrite] = true }
        opts.on("-s", "--source path", String, "Directory to the log files for visualizing") do |s|
          config[:source] = Pathname.new(s).expand_path
        end
        opts.on("-w", "--workspace path", String, "Directory for storing temporary files. Default: /tmp/rmv") do
          config[:workspace] = Pathname.new(w).expand_path
        end

        begin
          argv = ["-h"] if argv.empty?
          opts.parse! argv
        rescue OptionParser::ParseError => e
          STDERR.puts e.message, "\n", opts
          exit(-1)
        end
      end
    end

    private
      attr_accessor :config
  end
end

