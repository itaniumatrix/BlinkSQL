
import subprocess
import sys
from sys import stderr

# Note: commands inspired by AMPLab Benchmark (Patrick Windell)

# Run a command on a host through ssh, throwing an exception if ssh fails
def ssh(host, username, identity_file, command):
  subprocess.check_call(
      "ssh -t -o StrictHostKeyChecking=no -i %s %s@%s '%s'" %
      (identity_file, username, host, command), shell=True)



def prepare_hive_dataset(opts):
  def ssh_hive(command, user="root"):
    command = 'sudo -u %s %s' % (user, command)
    ssh(opts.hive_host, "root", opts.hive_identity_file, command)

  if not opts.skip_s3_import:
    print "=== IMPORTING BENCHMARK FROM S3 ==="
    try:
      ssh_hive("hadoop dfs -rmr -skipTrash /tmp/benchmark", user="hdfs")
      ssh_hive("hadoop dfs -rmr -skipTrash .Trash", user="hdfs")
      ssh_hive("hadoop dfs -expunge", user="hdfs")
      ssh_hive("hadoop dfs -mkdir /tmp/benchmark", user="hdfs")
    except Exception:
      pass # Folder may already exist

    cp_rankings = "hadoop distcp s3n://%s:%s@big-data-benchmark/pavlo/%s/%s/rankings/ " \
                  "/tmp/benchmark/rankings/" % (opts.aws_key_id,
                                                opts.aws_key,
                                                opts.file_format, opts.data_prefix)

    cp_uservisits = "hadoop distcp s3n://%s:%s@big-data-benchmark/pavlo/%s/%s/uservisits/ " \
                    "/tmp/benchmark/uservisits/" % (opts.aws_key_id,
                                                    opts.aws_key,
                                                    opts.file_format, opts.data_prefix)

    cp_crawl = "hadoop distcp s3n://%s:%s@big-data-benchmark/pavlo/%s/%s/crawl/ " \
               "/tmp/benchmark/crawl/" % (opts.aws_key_id,
                                          opts.aws_key,
                                          "text", opts.data_prefix)

    ssh_hive(cp_rankings, user='hdfs')
    ssh_hive(cp_uservisits, user='hdfs')
    ssh_hive(cp_crawl, user='hdfs')

  print "=== CREATING HIVE TABLES FOR BENCHMARK ==="