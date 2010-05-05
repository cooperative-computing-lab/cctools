#!/usr/bin/ruby

require "fileutils"

# TODO: real tests, check responses and detect failures

BUCKET_NAME = "s3cpp_testbucket"
$successes = 0
$failures = 0

FileUtils.mkdir("s3test")
FileUtils.cd("s3test")
FileUtils.ln_s("../s3tool", "s3tool")

def run(cmd)
    puts cmd
    system(cmd)
    if($? == 0)
        $successes += 1
        puts "+++SUCCESS+++"
    else
        $failures += 1
        puts "###FAILURE###"
    end
end

puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Installing in test directory"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3tool install")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Getting test image"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3get images.arklyffe.com spark.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Listing all buckets"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3ls")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Creating test bucket"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3mkbkt #{BUCKET_NAME}")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Putting spark.png"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3put #{BUCKET_NAME} spark.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Listing #{BUCKET_NAME}"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3ls #{BUCKET_NAME}")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Listing #{BUCKET_NAME} spark.png"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3ls #{BUCKET_NAME} spark.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Getting metadata for #{BUCKET_NAME} spark.png"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3getmeta #{BUCKET_NAME} spark.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Getting spark.png to localspark.png"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3get #{BUCKET_NAME} spark.png localspark.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Copying #{BUCKET_NAME}/spark.png to #{BUCKET_NAME}/sparkcopy.png"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3cp #{BUCKET_NAME} spark.png #{BUCKET_NAME} sparkcopy.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Moving #{BUCKET_NAME}/spark.png to #{BUCKET_NAME}/sparkoriginal.png"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3mv #{BUCKET_NAME} spark.png #{BUCKET_NAME} sparkoriginal.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Getting metadata for #{BUCKET_NAME} sparkoriginal.png"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3getmeta #{BUCKET_NAME} sparkoriginal.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Getting metadata for #{BUCKET_NAME} sparkcopy.png"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3getmeta #{BUCKET_NAME} sparkcopy.png")

# TODO:
# bucket to bucket move/copy

# Test permissions and metadata:
#TODO: s3put file -pPERM -tTYPE -mMETADATA
# s3putmeta
# move, copy with metadata override

# s3setbktacl
# s3setacl
# s3getacl

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Removing objects"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3rm #{BUCKET_NAME} sparkoriginal.png")
run("./s3rm #{BUCKET_NAME} sparkcopy.png")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Removing bucket"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3rmbkt #{BUCKET_NAME}")

puts ""
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
puts "Listing all buckets"
puts "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
run("./s3ls")

# rm s3test and contents

puts ""
puts ""
puts "#{$successes} successes, #{$failures} failures"
