require 'rmv'

module RMV
  class Page
    def initialize title="", base_path="/"
      @title = title
      @base = Pathname.new base_path
      @content = ""
      @path = ""
    end

    def to_s
      header << content << footer
    end

    def << s
      @content << s
    end

    def path= path
      path = Pathname.new path
      @path = base.relative_path_from path
    end

    def base= path
      @base = Pathname.new path
    end

    private
      attr_reader :title, :content, :base

      def header
        %Q{<!doctype html>
           <meta name="viewport" content="initial-scale=1.0, width=device-width" />
           <link rel="stylesheet" type="text/css" media="screen, projection" href="#{@path}/css/style.css" />
           <title>#{title}</title>

           <div class="content">
        }
      end

      def footer
        %Q{ </div> }
      end
  end
end
