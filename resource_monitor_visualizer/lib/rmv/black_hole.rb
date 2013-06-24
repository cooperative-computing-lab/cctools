require 'rmv'

module RMV
  class BlackHole
    def nil?
      true
    end

    def method_missing m, *a, &b
      self
    end
  end
end
