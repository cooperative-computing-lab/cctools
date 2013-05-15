require 'rmv'

module RMV
  class HistogramBuilder
    def initialize resources, workspace, destination, tasks
      @resources = resources
      @workspace = workspace
      @destination = destination
      @tasks = tasks
    end

    def build sizes=[[600,600]], output=""
      groups = find_groups
      @grouped_maximums = groups.map {|g| find_maximums g}
      results = []
      @grouped_maximums.each_with_index do |g, i|
        write_maximum_values(g, i){|a,b| scale_maximum a, b}
        results.concat(format_histograms sizes, i, groups[i])
      end
      results
    end

    def find_groups
      groups = []
      tasks.each do |t|
        exe = group_heuristic t
        groups << exe unless groups.include? exe
      end
      groups
    end

    private
      attr_reader :resources, :workspace, :destination, :tasks

      def group_heuristic task
        task.executable_name
      end

      def find_maximums group
        max = Hash[ @resources.map {|r| [r,[]] }]
        tasks.each do |t|
          resources.each do |r|
            if group_heuristic(t) == group
              tmp = t.max r.name.to_sym
              max[r].push tmp
            end
          end
        end
        max
      end

      def scale_maximum name, value
        value /= 1073741824.0 if name.match /byte/
        value /= 1024.0 if name.match /footprint/
        value /= 1024.0 if name.match /memory/
        value
      end

      def write_maximum_values maximum_list, index
        base_path = workspace + "group#{index}"
        base_path.mkpath
        maximum_list.each do |m|
          path = base_path + m.first.name.to_s
          File.open(path, 'w:UTF-8') do |f|
            m.last.each do |line|
              line = yield( m.first.name, line) if block_given?
              f.puts line
            end
          end
        end
      end

      def format_histograms sizes, group, group_name
        formatted = []
        sizes.each do |s|
          width = s.first
          height = s.last
          resources.each do |r|
            formatted << gnuplot_format(width, height, r, workspace+"group#{group}"+r.to_s, group, group_name)
          end
        end
        formatted
      end

      def gnuplot_format(width=600, height=600, resource="", data_path="/tmp", group=0, group_name="a")
        max = scale_maximum resource.name.to_s, @grouped_maximums[group][resource].max
        unit = resource.unit
        unit = "GB" if unit.match /MB/
        unit = "GB" if resource.name.match /footprint/
        unit = "MB" if unit.match /kB/
        unit = "GB" if resource.name.match /bytes/
        unit = " (#{unit}) " if unit != ""
        image_path = destination + "#{group_name}"
        image_path.mkpath
        image_path += "#{resource.name.to_s}_#{width}x#{height}_hist.png"
        binwidth = 1
        binwidth = max/40 unless max <= 40
        %Q{set terminal png transparent size #{width},#{height}
        set bmargin 4
        unset key

        set ylabel "Frequency"
        set output "#{image_path}"
        binwidth=#{binwidth}
        set boxwidth binwidth*0.9
        set style fill solid 0.5
        bin(x,width)=width*floor(x/width)
        set yrange [0:*]
        set xrange [0:*]
        set xtics right rotate by -45
        set xlabel "#{resource.name.to_s}#{unit}" offset 0,-2 character
        set bmargin 7
        plot "#{data_path.to_s}" using (bin(\$1,binwidth)):(1.0) smooth freq w boxes lc rgb"#5aabbc"
        }
      end
  end
end
