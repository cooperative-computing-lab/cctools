require 'rmv'

require 'yaml'

module RMV
  class TaskCollection
    def initialize summary_paths=[], time_series_paths=[]
      @tasks = summary_paths.zip(time_series_paths).map { |sp, tp| Task.new(sp,tp) }
    end

    def each &block
      tasks.send :each, &block
    end

    def last
      tasks.last
    end

    def first
      tasks.first
    end

    def select &block
      tasks.send :select, &block
    end

    private
      attr_reader :tasks
  end
end
