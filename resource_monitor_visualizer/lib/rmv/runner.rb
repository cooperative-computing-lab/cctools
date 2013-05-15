require 'rmv'

require 'pathname'
require 'yaml'
require 'open3'

module RMV
  class Runner
    attr_reader :debug, :source, :overwrite, :time_series, :resources, :workspace, :name, :tasks, :writer

    def initialize argv
      options = Options.new argv
      process_arguments options
      initialize_writers
    end

    def run
      find_files
      find_resources
      create_histograms
      create_group_resource_summaries
      make_combined_time_series
      @makeflow_log_exists = plot_makeflow_log source + 'Makeflow.makeflowlog'
      make_index [[1250,500]], [[600,600],[250,250]]
      copy_static_files
      remove_temp_files unless debug
    end

    private
      def remove_temp_files
        `rm -rf #{workspace}`
      end

      def initialize_writers
        @destination_writer = Writer.new @destination, overwrite
        @workspace_writer = Writer.new workspace, overwrite
      end

      def make_directories
        @groups.product(resources).map { |g, r| (@destination + "#{g}" + "#{r}").mkpath }
      end

      def create_group_resource_summaries histogram_size=[600,600]
        make_directories
        @groups.product(resources).map do |g, r|
          page = Page.new "#{name} Workflow"
          page << <<-INDEX
          <h1><a href="../../index.html">#{name}</a> - #{g} - #{r}</h1>
          <img src="../#{r.to_s}_#{histogram_size.first}x#{histogram_size.last}_hist.png" class="center" />
          <table>
          <tr><th>Rule Id</th><th>Maximum #{r}</th></tr>
          INDEX

          tasks.select{ |t| t.executable_name == g }.sort_by{ |t| t.grab r.name }.each do |t|
            rule_path = "#{g}/#{t.rule_id}.html"
            create_rule_page t, rule_path
            scaled_resource, _ = resources.scale r, (t.grab r.name)
            page << "<tr><td><a href=\"../#{t.rule_id}.html\">#{t.rule_id}</a></td><td>#{(scaled_resource*1000).round/1000.0}</td></tr>\n"
          end

          page << "</table>\n"

          write "#{g}/#{r}/index.html", page
        end
      end

      def write path, content, location=:destination
        case location
        when :destination
          @destination_writer.file path, content
        when :workspace
          @workspace_writer.file path, content
        else
          STDERR.puts "Cannot write to #{location}."
        end
      end

      def run_if_not_exist path
        yield if overwrite or !path.exist?
      end

      def scale_time_series task, scratch_file
        start = nil
        scratch_file.open("w:UTF-8") do |f|
          task.time_series.open.each do |l|
            next if l.match /#/
            l = l.split(/\s+/)
            next if l.length <= 1
            l = l.map {|a| a.to_i}
            start = l.first unless start
            l[0] = l.first - start
            resources.each_with_index do |r, i|
              l[i], _ = resources.scale r, l[i]
            end
            f.puts l.join(" ")
          end
        end
      end

      def create_rule_page task, path
        page = Page.new "#{name} Workflow"

        page << <<-INDEX
        <h1><a href="../../index.html">#{name}</a> - #{task.executable_name} - #{task.rule_id}</h1>
        <table>
        INDEX

        plot_task_timeseries task

        page << "<tr><td>command</td><td>#{task.grab "command"}</td></tr>\n"
        resources.each_with_index do |r, i|
          value, unit = resources.scale r, task.grab(r.name)
          img_path = "#{r.name}/#{task.rule_id}.png"
          page << "<tr><td><a href=\"#{r.name}/index.html\">#{r.name}</a></td><td>#{(value*1000).round/1000.0} #{unit}</td>"
          page << "<td><img src=\"#{img_path}\" /></td>" if i > 0
          page << "</tr>\n"
        end

        page << "</table>\n"

        write path, page
      end

      def plot_task_timeseries task
        scratch_file = @workspace + "#{task.rule_id}.scaled"
        run_if_not_exist(scratch_file) { scale_time_series task, scratch_file }

        resources.each_with_index {|r,i| task_resource_timeseries_plot r, i, task, scratch_file}
      end

      def task_resource_timeseries_plot resource, column, task, scratch_file
        unless column == 0
          out = @destination + "#{task.executable_name}" + "#{resource.name}"
          out.mkpath
          out += "#{task.rule_id}.png"
          run_if_not_exist(out) do
            gnuplot {|io| io.puts time_series_format(600, 300, resource, scratch_file, out, column+1, nil )}
          end
        end
      end

      def find_start_time
        t1 = tasks.first
        lowest = t1.grab :start
        t2 = tasks.last
        highest = t2.grab :start
        lowest < highest ? lowest : highest
      end

      def make_combined_time_series
        usage = find_aggregate_usage
        write_usage usage
        plot_time_series_summaries [[1250,500]]
      end

      def write_usage usage
        usage.each do |u|
          path = "aggregate_#{u.first.to_s}"
          output = []
          u.last.each {|k,v| output << "#{k}\t#{v}"}
          output = output.sort_by do |a|
            a = a.split(/\t/)
            a[0].to_i
          end
          write path, output, :workspace
        end
      end

      def plot_time_series_summaries sizes
        sizes.each do |s|
          width = s.first
          height = s.last
          @resources.each do |r|
            cpu_unit = nil
            cpu_unit = "%" if r.name.match /cpu_time/
            gnuplot {|io| io.puts time_series_format( width, height, r, workspace+"aggregate_#{r.name.to_s}", nil, 2, cpu_unit)}
          end
        end
      end

      def time_series_format(width=1250, height=500, resource="", data_path="/tmp", outpath=nil, column=2, cpu_unit=nil)
        _, unit = resources.scale(resource, 0) { |u| cpu_unit.nil? ? u : "%" }
        outpath = "#{@destination + resource.to_s}_#{width}x#{height}_aggregate.png" unless outpath
        %Q{set terminal png transparent size #{width},#{height}
        set bmargin 4
        unset key
        set xlabel "Time (seconds)" offset 0,-2 character
        set ylabel "#{resource.to_s}#{unit}" offset 0,-2 character
        set output "#{outpath}"
        set yrange [0:*]
        set xrange [0:*]
        set xtics right rotate by -45
        set bmargin 7
        plot "#{data_path.to_s}" using 1:#{column} w lines lw 5 lc rgb"#465510"
        }
      end

      def find_aggregate_usage
        start = find_start_time.to_i
        previous_cpu = 0
        interval = nil
        aggregate_usage = {}
        @resources.each {|r| aggregate_usage[r] = Hash.new 0}
        @time_series.each do |s|
          lines = s.open.each
          lines.each do |l|
            unless l.match /^#/
              data = l.split /\s+/
              interval = data[0].to_i - start if [0, nil].include? interval
              adjusted_start = data[0].to_i - start
              @resources.each_with_index do |r, i|
                scaled_value, _ = resources.scale r, data[i].to_i
                if r.name.match /cpu_time/
                  tmp = scaled_value
                  scaled_value -= previous_cpu
                  scaled_value /= interval if interval > 0
                  previous_cpu = tmp
                end
                aggregate_usage[r][adjusted_start] += scaled_value unless i == 0
              end
            end
          end
        end
        aggregate_usage
      end

      def process_arguments args
        %w(source destination workspace).each do |w|
         instance_variable_set "@#{w}", args.send(w)
        end

        @name = args.name
        @debug = args.debug
        @overwrite = args.overwrite

        @workspace.mkpath
        @top_level_destination = @destination
        @destination = @destination + @name.downcase
        @destination.mkpath unless @destination.exist?
      end

      def copy_static_files
        `cp -r lib/rmv_static/* #{@top_level_destination}`
      end

      def find_files
        time_series_paths = []
        summary_paths = []
        Pathname.glob(@source + "*.series")  { |p| time_series_paths << p }
        Pathname.glob(@source + "*.summary") { |p| summary_paths     << p }
        @time_series = time_series_paths
        @tasks = TaskCollection.new summary_paths, time_series_paths
      end

      def find_resources
        header = time_series.first.open(&:readline).chomp
        header = header[1..-1]
        @resources = Resources.new header
      end

      def create_histograms
        builder = HistogramBuilder.new resources, workspace, @destination, tasks
        @groups = builder.find_groups
        builder.build([[600,600],[250,250]]).map do |b|
          gnuplot { |io| io.puts b }
        end
      end

      def make_index summary_sizes = [[1250, 500]], histogram_sizes=[[600,600]]
        path = "index.html"
        page = Page.new "#{name} Workflow"
        page << " <h1>#{name} Workflow</h1>"

        summary_sizes = summary_sizes.sort_by{|s| s.first}
        summary_large = summary_sizes.last
        page << slides(summary_large.first, summary_large.last)

        histogram_sizes = histogram_sizes.sort_by {|s| s.first}
        hist_small = histogram_sizes.first
        hist_large = histogram_sizes.last
        @groups.each_with_index do |g, i|
          page << %Q{\n<hr />\n} if i > 0
          page << %Q{\n<h2>#{g}</h2>}
          @resources.each do |r|
            page << %Q{<a href="#{g}/#{r.to_s}/index.html"><img src="#{g}/#{r.to_s}_#{hist_small.first}x#{hist_small.last}_hist.png" /></a>\n}
          end
        end

        write path, page
      end

      def slides height, width
        result = <<-INDEX
        <script src=\"http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js\"></script>
        <script src=\"../js/slides.min.jquery.js\"></script>
        <script>\n \$(function(){\n \$('#slides').slides({\n preload: true,\n });\n });\n </script>
        <!-- javascript and some images licensed under Apache-2.0 by  Nathan Searles (http://nathansearles.com/) -->
        <section class="summary">
          <div id="slides">
            <div class="slides_container">
        INDEX
        result << %Q{<div class="slide"><div class="item"><img src="makeflowlog_1250x500.png" /></div></div>} if @makeflow_log_exists

        resources.each_with_index do |r, i|
          result << %Q{ <div class="slide"><div class="item"><img src="#{r.to_s}_#{height}x#{width}_aggregate.png" /></div></div>\n} unless i == 0
        end

        result << <<-INDEX
           </div>
            <a href=\"#\" class=\"prev\"><img src=\"../img/arrow-prev.png\" width=\"24\" height=\"43\" alt=\"Arrow Prev\"></a>
            <a href=\"#\" class=\"next\"><img src=\"../img/arrow-next.png\" width=\"24\" height=\"43\" alt=\"Arrow Next\"></a>
          </div>
        </section>
        INDEX
      end

      def gnuplot
        output = nil
        begin
          Open3::popen3 "gnuplot" do |i, o, e, t|
            yield i
            i.close_write
          end
        rescue Errno::ENOENT => e
          STDERR.puts "gnuplot not installed"
        end
        output
      end

      def plot_makeflow_log log_file
        if log_file.exist?
          mflog = MakeflowLog.from_file log_file
          write "summarydata", mflog, :workspace
          gnuplot {|io| io.puts mflog.gnuplot_format(1250, 500, workspace + "summarydata", @destination) }
        end
        log_file.exist?
      end
  end
end
