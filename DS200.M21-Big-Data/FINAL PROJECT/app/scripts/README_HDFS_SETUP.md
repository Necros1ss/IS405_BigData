HDFS setup & transfer - quick guide

Files added in this folder:
- `setup_hadoop_single_node.sh` : Bash script to install OpenJDK, Hadoop 3.3.6, configure pseudo-distributed HDFS/YARN and start services. Run on Lubuntu VM as root (sudo).
- `clean_and_transfer.ps1` : PowerShell script to normalize CSV to UTF-8 (no BOM), compute MD5, zip and `scp` to the VM.
- `csv_to_parquet.py` : Converter to turn CSV into Parquet (uses pyspark if available, otherwise pandas+pyarrow).

Quick run steps

1) On Lubuntu VM (run as root):
```bash
sudo bash setup_hadoop_single_node.sh
```
This will install packages, create `hadoop` user, configure minimal Hadoop settings, format NameNode (only once) and start HDFS/YARN.

2) On Windows (prepare and push file):
```powershell
# example
.
# run PowerShell from folder containing clean_and_transfer.ps1
.
.
pwsh ./clean_and_transfer.ps1 -LocalPath C:\path\to\your.csv -VmUser hadoop -VmHost 192.168.1.100 -RemotePath /home/hadoop/data/
```
Requirements: OpenSSH client on Windows (or use WinSCP / WSL rsync). The script writes `clean_<filename>` and `clean_<filename>.zip` and then scp.

3) On Lubuntu VM (after transfer):
```bash
# if zipped
unzip /home/hadoop/data/clean_your.csv.zip -d /home/hadoop/data/
# check md5 if needed
md5sum /home/hadoop/data/clean_your.csv
# put into HDFS
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -put /home/hadoop/data/clean_your.csv /user/hadoop/input/
hdfs dfs -ls /user/hadoop/input
```

4) Optional: convert CSV -> Parquet (faster analytics)
```bash
# prefer running the converter script (uses pyspark if available, otherwise pandas)
python3 app/scripts/csv_to_parquet.py /home/hadoop/data/clean_your.csv /home/hadoop/data/clean.parquet
```

Notes & tuning
- The setup script sets `dfs.replication=1` and moderate YARN values appropriate for ~4GB RAM VM. Edit `${HADOOP_HOME}/etc/hadoop/*-site.xml` to tweak.
- If you have limited RAM, reduce `yarn.nodemanager.resource.memory-mb` and mapreduce container settings.
- For analytics prefer Parquet + Snappy compression.

Additional scripts
- `tune_hadoop.sh` : adjust `*-site.xml` values for memory, vcores, blocksize, replication.
- `upload_sample.sh` : upload `app/data_final/youtube_sample.csv` to a VM and optionally put into HDFS.
- `monitor_hadoop.sh` : quick local checks (jps, hdfs dfsadmin -report, yarn node -list).

Examples

Tune Hadoop for a VM with 6GB RAM and 4 vCPUs:
```bash
sudo bash app/scripts/tune_hadoop.sh --mem 5120 --vcores 4 --map-mb 1024 --reduce-mb 2048 --replication 1
```

Transfer local sample to VM and put into HDFS (replace IP/user):
```bash
./app/scripts/upload_sample.sh --vm-host 192.168.56.101 --vm-user hadoop --hdfs
```

Start quick monitor:
```bash
bash app/scripts/monitor_hadoop.sh
```

Orchestrated full-run
--------------------
I added an orchestrator script that you run on the VM to perform all remaining steps: ensure Hadoop installed, start services, put your CSV into HDFS and run the Spark pipeline saving model and predictions to HDFS.

Run on the VM as `thinh` (example):
```bash
# adjust local CSV path if you copied via scp to /home/thinh/data/
sudo bash app/scripts/orchestrate_full_pipeline.sh /home/thinh/data/trending_yt_videos_113_countries.csv --hdfs-user thinh --num-trees 100 --max-depth 12
```

Upload from Windows (simple scp script):
```powershell
# run on Windows PowerShell
pwsh .\app\scripts\scp_upload_raw.ps1 -LocalCsv "C:\Users\Admin\.cache\kagglehub\datasets\asaniczka\trending-youtube-videos-113-countries\versions\925\trending_yt_videos_113_countries.csv" -VmUser thinh -VmHost 10.0.2.15
```

Spark output options
--------------------
The Spark pipeline now supports saving model, predictions, and metrics to flexible output locations.

**Predictions output formats:**
- `--save-predictions <path>`: save as Parquet (HDFS or local)
- `--save-predictions-csv <path>`: save as CSV (HDFS or local)

**Model and metrics:**
- `--save-model <path>`: save trained PipelineModel (HDFS or local)
- `--save-metrics <local-path>`: save AUC and feature importances as JSON

Example full run with outputs:
```bash
bash scripts/run_spark.sh "hdfs://localhost:9000/user/thinh/input/*.csv" \
  --no-sample --num-trees 100 --max-depth 12 \
  --save-model "hdfs://localhost:9000/user/thinh/models/rf" \
  --save-predictions "hdfs://localhost:9000/user/thinh/predictions/rf.parquet" \
  --save-predictions-csv "hdfs://localhost:9000/user/thinh/predictions/rf.csv" \
  --save-metrics "/tmp/rf_metrics.json"
```

