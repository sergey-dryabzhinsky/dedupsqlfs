# Простые тесты скорости работы DedupSQLfs v1.2.951 со сборкой через cython

Проверка после оптимизации структуры БД.

Распаковка LibreOffice 6.4.7 + 7.1.3

Проверка работы на движке: sqlite 3.34.1

CPU: i5-6600K @ 3.50GHz

System: Ubuntu 16.04 amd64

Kernel: 4.15.0-142-lowlatency

Python: 3.9.5 + cython 0.29.23

Размер LibreOffice в tar:

* 6.4.7 - 1133M
* 7.1.3 - 1183M

Другое: включен autocommit, выключен sync, hash=md5

Команда: python3.9 ./mount.dedupsqlfs -vv --verbose-stats --data ~/Temp/sqlfs/data/ --compress {method} --no-sync --no-cache-flusher --minimal-compress-size -1 ~/Temp/sqlfs/mount

Извлечение: tar xf {libre-tar} -C ~/Temp/sqlfs/mount

## Тесты

| method | untar 6.4.7: time, size, speed | untar 7.1.3: time, size, speed |
| ------ |:------------------------------:|:------------------------------:|
| none | 1:39.50, 1095M, 32.03 MiB/s | 1:33.57, 1641M, 36.40 MiB/s |
| *none* | 1:49.55, 1095M, 31.61 MiB/s | 1:44.10, 1641M, 30.81 MiB/s |
| zstd | 1:37.39, 404M, 31.06 MiB/s | 1:32.48, 585M, 32.67 MiB/s |
| *zstd* | 1:53.25, 404M, 23.22 MiB/s | 1:46.52, 585M, 30.91 MiB/s |

## Результат

Похоже, что с python3.9 сборка через cython замедляет работу.

Возможно cython помогает коду и более старым версиям python работать быстрее.
