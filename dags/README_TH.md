# ตัวอย่าง DAG (Demo DAGs)

> 🇬🇧 **English version: [README.md](README.md)** — ฉบับภาษาอังกฤษเป็นต้นฉบับ
> หากเนื้อหาขัดแย้งกัน ให้ยึดฉบับภาษาอังกฤษเป็นหลัก

ตัวอย่างการเขียน DAG บน Apache Airflow **3.x** ส่วนใหญ่เป็นงานรับส่งไฟล์ ตั้งแต่
อัปโหลดขึ้น FTPS, ใช้ sensor รอไฟล์, ไปจนถึงการ stream ไฟล์ระหว่างเซิร์ฟเวอร์
object storage และ SMB share — ครบทุกทิศทางระหว่าง FTPS, SFTP, SMB, Azure Blob
และ S3 โดย**ไม่มีการเขียนไฟล์ลงดิสก์ของ worker pod เลย** นอกจากนี้ยังมีอีก 3 ตัวที่
ไม่ใช่งานรับส่งไฟล์ ได้แก่ คู่ producer/consumer ของ queue, deadline alert และ
cyclic schedule

DAG เหล่านี้ยังไล่ระดับให้เห็นว่า **ควรใช้ของที่ provider มีให้มากแค่ไหน**:

| ระดับ | ตัวอย่าง |
|---|---|
| ใช้ operator ตามที่ provider ให้มาเลย | #1, #18 |
| สืบทอด sensor หรือสลับ hook | #2, #7, #9 |
| override เพียง method เดียวของ transfer operator | #4, #11, #14 |
| แก้ที่ *พฤติกรรม* ไม่ใช่ที่ท่อส่งข้อมูล | #16 — provider stream อยู่แล้ว แต่ลืม `prefetch` |
| เขียน operator ใหม่ทั้งตัว | #5, #6, #8, #12, #13 |

บทเรียนที่พบซ้ำ ๆ คือ **อ่านโค้ดของ provider ก่อนตัดสินใจ extend** — บางตัวแก้แค่
method เดียวก็พอ บางตัวไม่ต้องแก้อะไรเลย และบางตัว hook ถูก hardcode ไว้จนต้อง
เขียนใหม่ทั้ง method

DAG แต่ละตัวเป็น **ไฟล์เดียวจบ** ไม่มี helper module ร่วมกัน ไม่มีโฟลเดอร์ย่อย และ
ไม่ import ข้าม DAG — คัดลอกไฟล์เดียวออกไปใช้ที่อื่นก็ยังทำงานได้ ด้วยเหตุนี้
`MyFTPSHook` จึงปรากฏซ้ำในหลายไฟล์ ซึ่งเป็นความตั้งใจ

เขียนสำหรับการ deploy แบบ **KubernetesExecutor** ที่ทุก task รันใน pod ของตัวเอง
ข้อจำกัดนี้เป็นที่มาของการออกแบบเกือบทั้งหมด

---

## เลือกดูตามหมวด

ลำดับตัวเลขในเอกสารนี้คือ **ลำดับการรัน** (แต่ละตัวใช้ผลลัพธ์จากตัวก่อนหน้า)
ส่วนตารางด้านล่างคือชุดเดียวกันแต่จัดกลุ่มตามประเภทงาน

### Sensor — รอให้มีของเข้ามา

| # | DAG | เฝ้าดู |
|---|---|---|
| 2 | `dag_ftps_sensor.py` | ไฟล์ตามชื่อบน FTPS |
| 7 | `dag_wasb_prefix_suffix_sensor.py` | Azure Blob ทั้ง prefix **และ** suffix |
| 9 | `dag_s3_prefix_suffix_sensor.py` | S3 ทั้ง prefix **และ** suffix |
| 18 | `dag_sftp_sensor.py` | SFTP ด้วย `fnmatch` glob |

### Transfer — ส่งข้อมูลโดยไม่พักไฟล์ลงดิสก์

ทุกตัว stream ทั้งหมด ไม่มีตัวไหนเขียนไฟล์ลงดิสก์ของ worker pod

| จาก ↓ / ไป → | FTPS | SFTP | SMB | Blob | S3 |
|---|---|---|---|---|---|
| **local** | 1 | — | — | — | — |
| **FTPS** | — | 3 | — | 5 | — |
| **SFTP** | — | — | — | 4 | 16 |
| **Blob** | 8 | 6 | 12 | — | 10 |
| **S3** | 15 | 14 | 13 | 11 | — |

อ่านตารางนี้ตามแถวเทียบกับคอลัมน์ จะเห็นบทเรียนสำคัญของ repo นี้ทันที:
**ปลายทางคู่เดียวกันแต่คนละทิศทาง ใช้วิธีต่อท่อไม่เหมือนกัน** เช่น FTPS→Blob (#5)
ต้องใช้ `os.pipe()` แต่ Blob→FTPS (#8) ไม่ต้อง

### Messaging

| # | DAG | หน้าที่ |
|---|---|---|
| 19 | `dag_sqs_producer.py` | ส่งข้อความ แล้ว trigger #20 และรอจนเสร็จ |
| 20 | `dag_sqs_consumer.py` | อ่านข้อความและตรวจสอบ batch |

### Schedule และการแจ้งเตือน

| # | DAG | แสดงเรื่อง |
|---|---|---|
| 17 | `dag_deadline_alert.py` | แจ้งเตือนเมื่อ run ช้า **โดยไม่ทำให้ fail** |
| 21 | `dag_cyclic.py` | schedule ที่ไม่ให้ run ซ้อนกัน |

---

## ลำดับการรัน

DAG เหล่านี้ต่อยอดกัน ครั้งแรกควรรันจากบนลงล่าง

| # | DAG | แสดงเรื่อง | ต้องมีก่อน |
|---|---|---|---|
| 1 | `dag_ftps_simple_transfer.py` | ใช้ provider operator อัปโหลดไฟล์ | — |
| 2 | `dag_ftps_sensor.py` | sensor แบบ reschedule | ไฟล์จาก #1 |
| 3 | `dag_ftps_to_sftp_stream_transfer.py` | stream ระหว่างสองเซิร์ฟเวอร์ | ไฟล์จาก #1 |
| 4 | `dag_sftp_to_blob_stream.py` | stream เข้า object storage | ไฟล์ในโฟลเดอร์ต้นทาง SFTP |
| 5 | `dag_ftps_to_blob_stream.py` | เชื่อม API แบบ push เข้ากับแบบ pull | ไฟล์จาก #1 |
| 6 | `dag_blob_to_sftp_stream.py` | stream ออกจาก object storage | blob ใน container |
| 7 | `dag_wasb_prefix_suffix_sensor.py` | สืบทอด sensor ของ provider | blob ใน container |
| 8 | `dag_blob_to_ftps_stream.py` | โปรโตคอลเดิม แต่ทิศทางกลับกัน | blob ใน container |
| 9 | `dag_s3_prefix_suffix_sensor.py` | sensor แบบเดียวกันบน S3 | object ใน bucket |
| 10 | `dag_blob_to_s3_stream.py` | stream ข้ามคลาวด์ Azure → AWS | blob ใน container |
| 11 | `dag_s3_to_blob_stream.py` | ข้ามคลาวด์ทางกลับ ผ่าน provider operator | object ใน bucket |
| 12 | `dag_blob_to_smb_stream.py` | ปลายทางที่เป็น *writable* ไม่ใช่ readable | blob ใน container |
| 13 | `dag_s3_to_smb_stream.py` | เรียกครั้งเดียวจบ เพราะ SDK ทั้งสองฝั่งเข้ากันพอดี | object ใน bucket |
| 14 | `dag_s3_to_sftp_stream.py` | provider operator ตัวที่สามที่พักไฟล์ลงดิสก์ | object ใน bucket |
| 15 | `dag_s3_to_ftps_stream.py` | ต้อง override สองจุด ทั้ง logic และ hook | object ใน bucket |
| 16 | `dag_sftp_to_s3_stream.py` | provider stream อยู่แล้ว แต่ลืม `prefetch` | ไฟล์ในโฟลเดอร์ต้นทาง SFTP |
| 17 | `dag_deadline_alert.py` | แจ้งเตือน run ที่ช้าโดยไม่ทำให้ fail | — |
| 18 | `dag_sftp_sensor.py` | sensor ตัวเดียวที่ไม่ต้อง subclass | ไฟล์ในโฟลเดอร์ต้นทาง SFTP |
| 19 | `dag_sqs_producer.py` | trigger DAG อื่นแล้วรอจนเสร็จ | SQS queue |
| 20 | `dag_sqs_consumer.py` | อ่าน queue โดยถูกสั่งจาก #19 | ข้อความจาก #19 |
| 21 | `dag_cyclic.py` | run ตามเวลาโดยไม่ซ้อนกัน | — |

**เริ่มที่ #1** เพราะ #2 และ #3 ต้องการไฟล์ที่ #1 อัปโหลดไว้ก่อน ถ้ารันสลับลำดับ
sensor จะรอจนหมดเวลา และ transfer จะ fail ว่า "not found"

**#21 ไม่ต้องใช้ connection ใด ๆ** และเป็นตัวเดียวที่ทำงานตาม schedule ที่เหลือ
ต้อง trigger เอง

---

## ข้อกำหนดก่อนใช้งาน

### Airflow Connections

| Conn ID | ชนิด | ข้อมูลที่ต้องกรอก |
|---|---|---|
| `ftps_test_001` | **`FTP`** | host, login, password, port `21` |
| `sftp_test_001` | `SFTP` | host, login, password, port `22` |
| `wasb-nickstorageairflow002` | `wasb` | login = ชื่อ storage account, SAS token ใน extra |
| `aws_s3_test_001` | `aws` | login = access key id, password = secret, `{"region_name": "..."}` ใน extra |
| `aws_sqs_test_001` | `aws` | รูปแบบเดียวกับ S3 ส่วน queue URL ส่งตอนเรียกใช้ |
| `smb_test_001` | `samba` | host, **schema = ชื่อ share**, login, password |

> **ไม่มี connection type ชื่อ `FTPS`** — provider `ftp` ลงทะเบียน `conn_type="ftp"`
> ให้ทั้ง `FTPHook` และ `FTPSHook` การเลือกใช้ TLS ตัดสินจากโค้ดว่า import hook ตัวไหน
> ไม่ใช่จาก connection ดังนั้นเลือก `FTP` ใน UI ถูกต้องแล้ว

**`region_name` ไม่ใช่ตัวเลือกเสริม** — worker pod ไม่มี `AWS_DEFAULT_REGION` ดังนั้น
connection ที่ไม่ระบุ region จะใช้งานไม่ได้ใน cluster ถึงแม้ credential ชุดเดียวกันจะใช้
ได้จากเครื่อง laptop ที่ตั้ง profile ไว้แล้ว

### Airflow Variable

| Key | ค่า |
|---|---|
| `ftps_ca_cert` | PEM ของ CA certificate ของเซิร์ฟเวอร์ FTPS |

จำเป็นเฉพาะกรณีที่เซิร์ฟเวอร์ FTPS ใช้ใบรับรองแบบ self-signed หรือ private CA

### การระบุ host

ต้องใช้ address ที่ **worker pod** resolve ได้ — ชื่อ host ที่ใช้ได้จาก laptop
(ผ่าน VPN, `/etc/hosts`, หรือ mesh network) มักใช้ไม่ได้ภายใน cluster ให้ใช้ IP
หรือชื่อ DNS ภายใน cluster แทน

---

## วิธีรัน

ทุกตัวยกเว้น #21 ต้อง trigger เอง

```bash
airflow dags unpause <dag_id>          # DAG ใหม่จะถูก pause ไว้เสมอ
airflow dags trigger <dag_id> --conf '{...}'
```

ทุก DAG มีค่า default ที่ใช้ได้ทันที `--conf` จึงไม่บังคับ

### ทดสอบด้วยไฟล์ขนาดใหญ่

ไฟล์ `probe.txt` ที่ให้มามีขนาด 118 ไบต์ ซึ่งพิสูจน์ได้แค่ว่าต่อกันติด แต่บอกอะไร
เรื่อง throughput ไม่ได้ ให้สร้างไฟล์ใหญ่ขึ้นแล้วส่งชื่อผ่าน conf

```bash
# ไฟล์ทดสอบ 50 MiB — ใช้ข้อมูลสุ่ม ห้ามใช้ศูนย์ล้วน
# เพราะข้อมูลที่บีบอัดได้จะทำให้ TLS compression ทำให้ throughput ดูดีเกินจริง
dd if=/dev/urandom of=large50.bin bs=1m count=50
```

จากนั้น trigger ด้วยชื่อไฟล์นั้น เช่น

```bash
airflow dags trigger nix-dag-ftps-to-sftp-stream --conf '{"filename":"large50.bin"}'
airflow dags trigger nix-dag-blob-to-ftps-stream --conf '{"filename":"large50.bin","blob_prefix":"large/"}'
```

จุดที่มักพลาด 2 ข้อ:

- **#4 ต้องใช้ wildcard** เช่น `large50.*` ไม่ใช่ `large50.bin` เพราะการส่งชื่อไฟล์ตรง ๆ
  จะไปเจอกับดัก `listdir` ที่อธิบายไว้ในหน้าเอกสารของ DAG นั้น
- **`blob_prefix` ต้องตรงกับตำแหน่งจริงของ blob** ไฟล์ที่อัปโหลดไว้ใต้ `large/`
  ต้องใช้ `"blob_prefix":"large/"` ไม่ใช่ค่า default `incoming/`

ทุก transfer จะ log สรุปท้ายงานแบบนี้

```
[blob_to_ftps] done: 50.0 MiB in 3.7s (13.7 MiB/s) -> /upload/large50.bin
```

---

## "Streaming" ในที่นี้หมายถึงอะไร

หมายถึง **แบ่งเป็น chunk** เสมอ — ฝั่งหนึ่งอ่านหนึ่ง chunk อีกฝั่งเขียน chunk นั้น
แล้วจึงอ่าน chunk ถัดไป หน่วยความจำสูงสุดจึงเท่ากับหนึ่ง chunk ไฟล์ขนาด 2 GiB ใช้
หน่วยความจำเท่ากับไฟล์ 2 KiB และ **ไม่มีการเขียนไฟล์ลงดิสก์ของ worker pod เลย**

ขนาด chunk ขึ้นกับ call ที่เป็นตัวขับ loop จึงต่างกันไปในแต่ละ DAG

| DAG | ตัวแบ่ง chunk | ขนาด |
|---|---|---|
| #3 FTPS → SFTP | `retrbinary` → pipe → `putfo` | 8 KiB |
| #4 SFTP → Blob | Azure block upload | 4 MiB |
| #5 FTPS → Blob | `retrbinary` → pipe → Azure upload | 8 KiB |
| #6 Blob → SFTP | `putfo` | 32 KiB |
| #8 Blob → FTPS | `storbinary` | 8 KiB |
| #10, #13, #14, #16 | boto3 multipart | 8 MiB |

ในไฟล์โค้ดจะมีคอมเมนต์ `BYTE PATH` กำกับไว้ตรงจุดที่ข้อมูลไหลจริง เพื่อให้เห็นเส้นทาง
ตั้งแต่ socket ต้นทางถึงปลายทาง

**มีข้อยกเว้นหนึ่งข้อ** — #5 (FTPS → Blob) ไม่ได้ใช้หน่วยความจำคงที่กับไฟล์เล็ก
เพราะ Azure SDK จะ `read()` ทั้งก้อนสำหรับ blob ที่ไม่เกิน `max_single_put_size`
(ค่า default 64 MiB) ถ้าใหญ่กว่านั้นจึงเปลี่ยนเป็น block upload ที่ stream จริง

---

## เขียนไฟล์ชั่วคราวแล้วค่อยเปลี่ยนชื่อ

DAG ที่ปลายทางเป็นระบบไฟล์ (SFTP, FTPS, SMB — #3, #6, #8, #12, #13) จะเขียนลง
`<ชื่อไฟล์>.part` ก่อน แล้วค่อยเปลี่ยนชื่อเมื่อไบต์สุดท้ายถึงปลายทาง เพื่อให้ระบบที่มา
เฝ้าดูโฟลเดอร์ไม่เห็นไฟล์ที่ยังเขียนไม่เสร็จ และถ้า run ล้มเหลวก็จะเหลือไฟล์ `.part`
ที่เห็นชัดว่าไม่สมบูรณ์

| ปลายทาง | การเปลี่ยนชื่อ | ต้นทุน | atomic หรือไม่ |
|---|---|---|---|
| SFTP | `posix_rename()` | ฟรี | ใช่ |
| FTPS | `rename()` (RNFR/RNTO) | ฟรี | ใช่ ถ้าเซิร์ฟเวอร์รองรับ |
| SMB ผ่าน Samba | `unlink` + `rename()` | ฟรี | **ไม่** มีช่วงสั้น ๆ ที่ไม่มีชื่อไหนถือไฟล์อยู่ |
| Azure Blob | ไม่มี | ต้อง copy ทั้งก้อนฝั่งเซิร์ฟเวอร์ | — |
| S3 | ไม่มี | ต้อง copy ทั้งก้อนฝั่งเซิร์ฟเวอร์ | — |

DAG ที่ปลายทางเป็น object storage จึง**ไม่ใช้**วิธีนี้ เพราะไม่มีคำสั่ง rename และ
object จะยังไม่ปรากฏจนกว่าการอัปโหลดจะ commit อยู่แล้ว

**บน SFTP ต้องใช้ `posix_rename` ไม่ใช่ `rename`** เพราะ `rename` ธรรมดาจะ fail
เมื่อปลายทางมีไฟล์อยู่แล้ว ซึ่งทำให้การ retry พัง

---

## สิ่งที่ parse check จับไม่ได้

บั๊กทุกตัวที่บันทึกไว้ในเอกสารชุดนี้ **ผ่าน** การ parse และผ่าน integrity test
ทั้งหมด แต่ไปพังตอนรันจริงกับเซิร์ฟเวอร์

| อาการ | จุดที่ระเบิด |
|---|---|
| ส่ง path ที่เป็นไฟล์ ให้กับที่ที่ต้องการโฟลเดอร์ | `listdir` ครั้งแรก |
| ใช้ hook ที่ connection ถูกปิดไปแล้ว | คำสั่งแรกที่เรียกผ่าน client นั้น |
| ส่ง kwarg ที่ SDK ส่งต่อไปถึง HTTP transport | ตอนยิง request |
| provider ไม่มีอยู่ใน image ของเซิร์ฟเวอร์ | ตอน import DAG |

ให้ถือว่า parse check เป็นแค่ตัวกรองเบื้องต้น **การรันจริงครั้งแรกคือการทดสอบจริง**

สำหรับโค้ดที่ใช้ thread เช่น `os.pipe()` ใน #5 การ parse ไม่พิสูจน์อะไรเลย เพราะ
deadlock หรือ exception ที่ถูกกลืนหายจะดูเหมือนโค้ดที่ทำงานปกติจนกว่าจะรันจริง

---

## ข้อจำกัด 2 ข้อที่กำหนดรูปแบบของทุก DAG

### task แต่ละตัวไม่ได้ใช้ดิสก์ร่วมกัน

ภายใต้ KubernetesExecutor ทุก task รันใน pod ของตัวเองที่หายไปเมื่อจบงาน ไฟล์ที่
task หนึ่งเขียนลงดิสก์จะไม่มีอยู่สำหรับ task ถัดไป และจะ fail ด้วย
`FileNotFoundError` ตอนรันจริงเท่านั้น — parse check จับไม่ได้

ทางเลือกเรียงตามลำดับที่แนะนำ:

1. **อ่านจาก storage ที่ mount ร่วมกัน** — โฟลเดอร์ DAG ถูก mount เข้าไปในทุก pod
2. **ทำให้จบใน task เดียว** — สร้างข้อมูลในหน่วยความจำแล้วส่ง buffer ให้ hook
3. **ส่งตำแหน่งอ้างอิงแทนไฟล์** — เขียนขึ้น object storage แล้วส่ง key ผ่าน XCom

### ใบรับรองแบบ self-signed ต้องเขียน hook เอง

`FTPSHook.get_conn()` ของ Airflow hardcode `ssl.create_default_context()` โดยไม่มี
ช่องให้ใส่ CA ทำให้ใบรับรอง self-signed fail ด้วย `CERTIFICATE_VERIFY_FAILED`
และไม่มี setting ใดใน connection แก้ได้

DAG ที่ใช้ FTPS ในชุดนี้จึง subclass hook เอง โดย**ยังคงเปิดการตรวจสอบใบรับรองไว้**
(เพิ่ม CA เข้าไป ไม่ใช่ปิดการตรวจสอบ) และเรียก `prot_p()` ซึ่ง hook เดิมไม่ได้เรียก
ทำให้ช่องทางส่งข้อมูลถูกเข้ารหัสด้วย

---

## ข้อควรรู้เรื่อง Airflow 3.x

ตัวอย่างทั้งหมดใช้ได้กับ 3.x เท่านั้น จุดที่มักพลาดตอนย้ายจาก 2.x

```python
from airflow.providers.standard.operators.python import PythonOperator   # ✅ 3.x
from airflow.operators.python import PythonOperator                      # ❌ 2.x

from airflow.sdk import Variable                                         # ✅ 3.x
from airflow.models import Variable                                      # ❌ fail ใน worker pod

schedule=None                                                            # ✅
schedule_interval=None                                                   # ❌ ถูกถอดออกแล้ว
```

path แบบ 2.x ยังทำงานได้ในฐานะ deprecation shim จึง parse ผ่านและรันได้ ไม่มีอะไร
บอกชัดว่าผิด แต่ integrity test ของ repo นี้จะ fail ทันที

---

## การแก้ปัญหาที่พบบ่อย

| อาการ | สาเหตุ |
|---|---|
| `CERTIFICATE_VERIFY_FAILED` | ใช้ `FTPSHook` เดิมกับใบรับรอง self-signed หรือไม่ได้ตั้ง `ftps_ca_cert` |
| `FileNotFoundError: /tmp/...` | เข้าใจผิดว่า task สองตัวใช้ดิสก์ร่วมกัน |
| `550` จาก FTPS | ไม่มีไฟล์ต้นทาง ให้รัน #1 ก่อน |
| `553 Could not create file` | เขียนลงโฟลเดอร์ที่ไม่มีสิทธิ์เขียน |
| การส่งข้อมูลค้างหลัง login สำเร็จ | worker เข้าถึงช่วง port ของ passive mode ไม่ได้ |
| `FileNotFoundError` จาก `listdir_attr` ทั้งที่ไฟล์มีอยู่ | ส่งชื่อไฟล์ตรง ๆ ให้ #4 ต้องเป็นโฟลเดอร์หรือ `*` |
| `OSError: Socket is closed` ระหว่างส่งข้อมูล | ใช้ `get_conn()` หลังจาก session ถูกปิดไปแล้ว |
| `STATUS_ACCESS_DENIED` ตอนเขียน SMB | สิทธิ์ของโฟลเดอร์บนเซิร์ฟเวอร์ ไม่ใช่ปัญหาของ Airflow |
| DAG ไม่ขึ้นใน UI แต่ไม่มี import error | dag-processor ยังไม่ได้ parse ไฟล์ รออีกสักครู่ |
| DAG ขึ้นแล้วแต่ไม่รัน | ยังถูก pause อยู่ |

ตรวจ import error ก่อนเสมอ เพราะถ้ามีรายการในนี้ อย่างอื่นไม่มีความหมาย

```bash
airflow dags list-import-errors
```

---

## รายละเอียดของแต่ละ DAG

เอกสารเชิงลึกของแต่ละ DAG (รูปแบบที่ใช้, กับดักที่เจอ, log จริงจากการรัน) อยู่ใน
โฟลเดอร์ `docs/` เป็นภาษาอังกฤษ

| # | DAG | เอกสาร |
|---|---|---|
| 1 | `dag_ftps_simple_transfer.py` | [docs](docs/dag_ftps_simple_transfer.md) |
| 2 | `dag_ftps_sensor.py` | [docs](docs/dag_ftps_sensor.md) |
| 3 | `dag_ftps_to_sftp_stream_transfer.py` | [docs](docs/dag_ftps_to_sftp_stream_transfer.md) |
| 4 | `dag_sftp_to_blob_stream.py` | [docs](docs/dag_sftp_to_blob_stream.md) |
| 5 | `dag_ftps_to_blob_stream.py` | [docs](docs/dag_ftps_to_blob_stream.md) |
| 6 | `dag_blob_to_sftp_stream.py` | [docs](docs/dag_blob_to_sftp_stream.md) |
| 7 | `dag_wasb_prefix_suffix_sensor.py` | [docs](docs/dag_wasb_prefix_suffix_sensor.md) |
| 8 | `dag_blob_to_ftps_stream.py` | [docs](docs/dag_blob_to_ftps_stream.md) |
| 9 | `dag_s3_prefix_suffix_sensor.py` | [docs](docs/dag_s3_prefix_suffix_sensor.md) |
| 10 | `dag_blob_to_s3_stream.py` | [docs](docs/dag_blob_to_s3_stream.md) |
| 11 | `dag_s3_to_blob_stream.py` | [docs](docs/dag_s3_to_blob_stream.md) |
| 12 | `dag_blob_to_smb_stream.py` | [docs](docs/dag_blob_to_smb_stream.md) |
| 13 | `dag_s3_to_smb_stream.py` | [docs](docs/dag_s3_to_smb_stream.md) |
| 14 | `dag_s3_to_sftp_stream.py` | [docs](docs/dag_s3_to_sftp_stream.md) |
| 15 | `dag_s3_to_ftps_stream.py` | [docs](docs/dag_s3_to_ftps_stream.md) |
| 16 | `dag_sftp_to_s3_stream.py` | [docs](docs/dag_sftp_to_s3_stream.md) |
| 17 | `dag_deadline_alert.py` | [docs](docs/dag_deadline_alert.md) |
| 18 | `dag_sftp_sensor.py` | [docs](docs/dag_sftp_sensor.md) |
| 19 | `dag_sqs_producer.py` | [docs](docs/dag_sqs_producer.md) |
| 20 | `dag_sqs_consumer.py` | [docs](docs/dag_sqs_consumer.md) |
| 21 | `dag_cyclic.py` | [docs](docs/dag_cyclic.md) |

---

🇬🇧 **ฉบับเต็มภาษาอังกฤษ: [README.md](README.md)** — มีรายละเอียดมากกว่าฉบับนี้
