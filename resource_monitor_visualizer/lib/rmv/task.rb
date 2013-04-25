require_relative '../rmv'

module RMV
  class Task
    def initialize summary_path, time_series_path
      @summary_path = summary_path
      @summary = load_summary
      @time_series_path = time_series_path
    end

    def rule_id
      summary_path.to_s.match(/log-rule-(\d+)-summary/)[1]
    end

    def executable_name
      summary.executable_name
    end

    def max resource
      grab resource
    end

    def grab label
      summary.send label.to_sym
    end

    def time_series
      time_series_path
    end

    private
      attr_reader :summary, :summary_path, :time_series_path

      def load_summary
        Summary.new (YAML.load_file summary_path)
      end
  end
end
