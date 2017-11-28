class Distmap < Formula
  desc "A wrapper around different mappers for distributed computation using Hadoop."
  homepage "https://sourceforge.net/projects/distmap/"
  head "https://github.com/robmaz/distmap.git"

  def install
    bin.install "bin/distmap"
    lib.install Dir.glob("lib/*")
    inreplace "#{bin}/distmap", "$LibDir = /usr/lib/distmap", "$LibDir = #{lib}/distmap"
  end

  test do
    system "#{bin}/distmap", "--help"
  end
end
