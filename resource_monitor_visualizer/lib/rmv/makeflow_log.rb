require 'rmv'

module RMV
  class MakeflowLog
    class << self
      def from_file path
        lines = []
        path.open.each { |l| lines << l }
        data = process_lines lines
        MakeflowLog.new data
      end

      private
        def process_lines lines
          start = find_start_time lines
          lines.map {|l| process_line l, start}.compact
        end

        def find_start_time lines
          start = lines.select {|l| !comment?(l)}.first
          start = start.split(' ').first
          scale_time start
        end

        def scale_time t
          t.to_f/1000000.0
        end

        def process_line line, start
          unless comment? line
            l = line.split(' ')
            time = scale_time(l[0]) - start
            submitted = l[5..8].map{|a| a.to_i}.inject(:+)
            running = l[5]
            complete = l[6]
            "#{time} #{submitted} #{running} #{complete}"
          end
        end

        def comment? line
          line.match /^#/
        end
    end

    def initialize data
      @data = data
    end

    def to_s
      data.join "\n"
    end

    def gnuplot_format(width=1250, height=500, data_path="/tmp", output_path="")
      %Q{set terminal png transparent size #{width},#{height}
      set bmargin 4
      set key ins left top
      set xlabel "Time (seconds)" offset 0,-2 character
      set ylabel "Number of Jobs" offset 0,-2 character
      set output "#{output_path + 'makeflowlog'}_#{width}x#{height}.png"
      set xrange [0:*]
      set yrange [0:*]
      set xtics right rotate by -45
      set bmargin 7
      plot "#{data_path.to_s}" using 1:2 title "submitted" w lines lw 5 lc rgb"#465510", "" using 1:3 title "running" w lines lw 5 lc rgb"#BA6F2E", "" using 1:4 title "complete" w lines lw 5 lc rgb"#AA272F"
      }
    end

    private
      def data
        @data
      end
  end
end
