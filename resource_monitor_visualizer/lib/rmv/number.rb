require 'rmv'

module RMV
  class Number
    attr_reader :value, :unit

    def initialize value, unit
      @value = value.to_f
      if unit.to_s.length == 2
        @unit = unit
      else
        @unit = ""
      end
    end

    def to_f
      value.to_f
    end

    def <=> other
      value <=> other.value
    end

    def < other
      value < other.value
    end

    def prefix
      unit[0]
    end

    def base_value
      case prefix
      when 'u'
        value * 10**(-6)
      when 'K'
        value * 1024
      when 'M'
        value * 1024**2
      when 'G'
        value * 1024**3
      when 'T'
        value * 1024**4
      else
        value
      end
    end

    def in new_unit
      new_prefix = new_unit.to_s[0]
      case new_prefix
      when 'u'
        base_value * 10**6
      when 'K'
        base_value / 1024.0
      when 'M'
        base_value / 1024.0**2
      when 'G'
        base_value / 1024.0**3
      when 'T'
        base_value / 1024.0**4
      else
        value
      end
    end
  end
end
