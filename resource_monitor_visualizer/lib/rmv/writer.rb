require_relative "../rmv"

require 'pathname'

module RMV
  class Writer
    def initialize workspace, output_directory, overwrite
      @workspace = workspace
      @tld = output_directory
      @overwrite = overwrite
    end

    def file path, content
      out_path = compute_resultant_path path
      content = set_paths content, out_path
      run_if_not_exist out_path do
        out_path.open("w:UTF-8") { |f| f.puts content }
      end
    end

    private
      attr_reader :workspace, :tld, :overwrite

      def compute_resultant_path path
        if path.respond_to? :dirname
          return tld.relative_path_from (path.dirname)
        else
          return tld + path
        end
      end

      def set_paths content, out_path
        if content.class == Page
          content.base=tld
          content.path=out_path
        end
        content
      end

      def run_if_not_exist path
        yield if overwrite or !path.exist?
      end

  end
end
