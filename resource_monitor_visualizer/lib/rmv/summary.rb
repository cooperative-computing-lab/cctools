require 'rmv'

require 'yaml'

module RMV
  class Summary
    class << self
      def from_file path
         Summary.new(YAML.load_file(path))
      end

    end

    def initialize contents
      @contents = contents
    end

    def executable_name
      contents.fetch("command").split(' ').first.split(/\.\//).last
    end

    private
      def method_missing m, *a, &b
        if contents.include? m.to_s.gsub(/clock/, 'time')
          value = contents.fetch(m.to_s).to_s.gsub(/clock/, 'time').split(' ')
          Number.new value.first, value.last
        else
          super
        end
      end

      def contents
        @contents
      end
  end
end
