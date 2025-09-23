# yellow_pages/pipelines.py
from itemadapter import ItemAdapter
import logging
import pymysql
from pymysql.err import OperationalError, IntegrityError
from scrapy.exceptions import NotConfigured
from scrapy import signals


class YellowPagesPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        name = adapter.get("name")
        if name:
            adapter["name"] = name.strip()
        return item


class MySQLPipeline:
    def __init__(self, host, port, user, password, db, charset, use_unicode):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        self.use_unicode = use_unicode
        self.conn = None
        self.cursor = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        host = settings.get("MYSQL_HOST")
        port = settings.getint("MYSQL_PORT", 3306)
        user = settings.get("MYSQL_USER")
        password = settings.get("MYSQL_PASSWORD")
        db = settings.get("MYSQL_DATABASE")
        charset = settings.get("MYSQL_CHARSET", "utf8mb4")
        use_unicode = settings.getbool("MYSQL_USE_UNICODE", True)

        if not (host and user and db):
            raise NotConfigured("MySQLPipeline: missing MYSQL_HOST, MYSQL_USER or MYSQL_DATABASE in settings")

        pipe = cls(host, port, user, password, db, charset, use_unicode)
        # connect signals using scrapy.signals names
        crawler.signals.connect(pipe.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(pipe.close_spider, signal=signals.spider_closed)
        return pipe

    def open_spider(self, spider):
        try:
            self.conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db,
                charset=self.charset,
                use_unicode=self.use_unicode,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False,
            )
            self.cursor = self.conn.cursor()
            self.logger.info("MySQL connection opened")
        except OperationalError as e:
            self.logger.exception("Failed to connect to MySQL: %s", e)
            raise

    def close_spider(self, spider):
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
        if self.conn:
            try:
                self.conn.commit()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass
            self.logger.info("MySQL connection closed")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        url = adapter.get("url")
        if not url:
            self.logger.warning("Skipping item without URL: %r", dict(adapter.items()))
            return item  # skip DB insert

        # get values; normalize empty strings to None
        def norm(key):
            v = adapter.get(key)
            if v is None:
                return None
            if isinstance(v, str):
                v = v.strip()
                return v if v != "" else None
            return v

        name = norm("name")
        phone = norm("phone")
        address = norm("address")
        monday = norm("monday")
        tuesday = norm("tuesday")
        wednesday = norm("wednesday")
        thursday = norm("thursday")
        friday = norm("friday")
        saturday = norm("saturday")
        sunday = norm("sunday")

        sql = """
            INSERT INTO businesses
            (url, name, phone, address, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                phone = VALUES(phone),
                address = VALUES(address),
                monday = VALUES(monday),
                tuesday = VALUES(tuesday),
                wednesday = VALUES(wednesday),
                thursday = VALUES(thursday),
                friday = VALUES(friday),
                saturday = VALUES(saturday),
                sunday = VALUES(sunday)
        """

        params = (url, name, phone, address, monday, tuesday, wednesday, thursday, friday, saturday, sunday)

        try:
            self.cursor.execute(sql, params)
            self.conn.commit()
            self.logger.debug("Inserted/Updated: %s", url)
        except IntegrityError as e:
            self.logger.warning("Integrity error when inserting %s: %s", url, e)
            try:
                self.conn.rollback()
            except Exception:
                pass
        except OperationalError as e:
            self.logger.error("Operational error, trying reconnect: %s", e)
            try:
                self.reconnect()
                self.cursor.execute(sql, params)
                self.conn.commit()
            except Exception as e2:
                self.logger.exception("Failed after reconnect: %s", e2)
                try:
                    self.conn.rollback()
                except Exception:
                    pass
        except Exception as e:
            self.logger.exception("Unexpected error while inserting item: %s", e)
            try:
                self.conn.rollback()
            except Exception:
                pass

        return item

    def reconnect(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.db,
            charset=self.charset,
            use_unicode=self.use_unicode,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        self.cursor = self.conn.cursor()
        self.logger.info("MySQL reconnected")
