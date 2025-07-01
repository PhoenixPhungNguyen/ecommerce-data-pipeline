import duckdb

class S3CsvFileProcessor:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.conn = duckdb.connect()
        self.conn.execute(
            f"""
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{self.aws_access_key_id}',
                SECRET '{self.aws_secret_access_key}',
                REGION 'ap-southeast-2'
            )
            """
        )
        self.table_cleaning_rules = {
            "customers": self._process_customers,
            "order_items": self._process_order_items,
            "order_payments": self._process_order_payments,
            "order_reviews": self._process_order_reviews,
            "orders": self._process_orders,
            "product_category_name_translation": self._process_product_category_name_translation,
            "products": self._process_products,
            "sellers": self._process_sellers,
        }

    def _process_file(self, s3_raw_file_url):
        bucket_name = s3_raw_file_url.split("/")[2]
        table_name = s3_raw_file_url.split("/")[-1].split(".")[0]
        # Read the CSV file from S3
        self.conn.execute(
            f"CREATE OR REPLACE TABLE '{table_name}' AS SELECT DISTINCT * FROM read_csv_auto('{s3_raw_file_url}')"
        )
        cleaning_function = self._get_cleaning_function(table_name)
        cleaning_function()
        s3_processed_file_url = f"s3://{bucket_name}/processed/{table_name}.parquet"
        self._save_to_s3(table_name, s3_processed_file_url)
        return s3_processed_file_url

    def _get_cleaning_function(self, table_name):
        if table_name in self.table_cleaning_rules:
            return self.table_cleaning_rules.get(table_name)
        else:
            raise ValueError(f"No cleaning function defined for {table_name}")

    def _save_to_s3(self, table_name, s3_processed_file_url):
        self.conn.execute(
            f"""
            COPY {table_name}
            TO '{s3_processed_file_url}'
            (FORMAT 'parquet');
            """
        )

    def _process_customers(self):
        self.conn.execute("""
            CREATE OR REPLACE TABLE customers_convert AS
            SELECT DISTINCT
                CAST(customer_id AS TEXT) AS customer_id,
                CAST(customer_unique_id AS TEXT) AS customer_unique_id,
                CAST(customer_zip_code_prefix AS TEXT) AS customer_zip_code_prefix,
                LOWER(TRIM(customer_city)) AS customer_city,
                UPPER(TRIM(customer_state)) AS customer_state
            FROM customers;
        """)
        self.conn.execute("DROP TABLE customers;")
        self.conn.execute("ALTER TABLE customers_convert RENAME TO customers;")

    def _process_order_items(self):
        self.conn.execute("""
            CREATE OR REPLACE TABLE order_items_convert AS
            SELECT DISTINCT
                CAST(order_id AS TEXT) AS order_id,
                CAST(order_item_id AS INTEGER) AS order_item_id,
                CAST(product_id AS TEXT) AS product_id,
                CAST(seller_id AS TEXT) AS seller_id,
                strftime(shipping_limit_date, '%Y-%m-%dT%H:%M:%S') AS shipping_limit_date,
                CAST(price AS DOUBLE) AS price,
                CAST(freight_value AS DOUBLE) AS freight_value
            FROM order_items;
        """)
        self.conn.execute("DROP TABLE order_items;")
        self.conn.execute("ALTER TABLE order_items_convert RENAME TO order_items;")

    def _process_order_payments(self):
        self.conn.execute("""
            CREATE OR REPLACE TABLE order_payments_convert AS
            SELECT DISTINCT
                CAST(order_id AS TEXT) AS order_id,
                CAST(payment_sequential AS INTEGER) AS payment_sequential,
                LOWER(TRIM(payment_type)) AS payment_type,
                CAST(payment_installments AS INTEGER) AS payment_installments,
                CAST(payment_value AS DOUBLE) AS payment_value
            FROM order_payments;
        """)
        self.conn.execute("DROP TABLE order_payments;")
        self.conn.execute("ALTER TABLE order_payments_convert RENAME TO order_payments;")

    def _process_order_reviews(self):
        self.conn.execute("""
            CREATE OR REPLACE TABLE order_reviews_convert AS
            SELECT DISTINCT
                CAST(review_id AS TEXT) AS review_id,
                CAST(order_id AS TEXT) AS order_id,
                CAST(review_score AS INTEGER) AS review_score,
                TRIM(review_comment_title) AS review_comment_title,
                TRIM(review_comment_message) AS review_comment_message,
                strftime(review_creation_date, '%Y-%m-%d') AS review_creation_date,
                strftime(review_answer_timestamp, '%Y-%m-%dT%H:%M:%S') AS review_answer_timestamp
            FROM order_reviews;
        """)
        self.conn.execute("DROP TABLE order_reviews;")
        self.conn.execute("ALTER TABLE order_reviews_convert RENAME TO order_reviews;")

    def _process_orders(self):
        self.conn.execute("""
           CREATE OR REPLACE TABLE orders_convert AS
            SELECT DISTINCT
                CAST(order_id AS TEXT) AS order_id,
                CAST(customer_id AS TEXT) AS customer_id,
                LOWER(TRIM(order_status)) AS order_status,
                strftime(order_purchase_timestamp, '%Y-%m-%dT%H:%M:%S') AS order_purchase_timestamp,
                strftime(order_approved_at, '%Y-%m-%dT%H:%M:%S') AS order_approved_at,
                strftime(order_delivered_carrier_date, '%Y-%m-%dT%H:%M:%S') AS order_delivered_carrier_date,
                strftime(order_delivered_customer_date, '%Y-%m-%dT%H:%M:%S') AS order_delivered_customer_date,
                strftime(order_estimated_delivery_date, '%Y-%m-%dT%H:%M:%S') AS order_estimated_delivery_date
            FROM orders;
        """)
        self.conn.execute("DROP TABLE orders;")
        self.conn.execute("ALTER TABLE orders_convert RENAME TO orders;")

    def _process_product_category_name_translation(self):
        self.conn.execute("""
           CREATE OR REPLACE TABLE product_category_name_translation_convert AS
            SELECT DISTINCT
                TRIM(product_category_name) AS product_category_name,
                TRIM(product_category_name_english) AS product_category_name_english
            FROM product_category_name_translation;
        """)
        self.conn.execute("DROP TABLE product_category_name_translation;")
        self.conn.execute("ALTER TABLE product_category_name_translation_convert RENAME TO product_category_name_translation;")

    def _process_products(self):
        self.conn.execute("""
           CREATE OR REPLACE TABLE products_convert AS
            SELECT DISTINCT
                CAST(product_id AS TEXT) AS product_id,
                TRIM(product_category_name) AS product_category_name,
                CAST(product_name_lenght AS INTEGER) AS product_name_length,
                CAST(product_description_lenght AS INTEGER) AS product_description_length,
                CAST(product_photos_qty AS INTEGER) AS product_photos_qty,
                CAST(product_weight_g AS INTEGER) AS product_weight_g,
                CAST(product_length_cm AS INTEGER) AS product_length_cm,
                CAST(product_height_cm AS INTEGER) AS product_height_cm,
                CAST(product_width_cm AS INTEGER) AS product_width_cm
            FROM products;
        """)
        self.conn.execute("DROP TABLE products;")
        self.conn.execute("ALTER TABLE products_convert RENAME TO products;")

    def _process_sellers(self):
        self.conn.execute("""
           CREATE OR REPLACE TABLE sellers_convert AS
            SELECT DISTINCT
                CAST(seller_id AS TEXT) AS seller_id,
                CAST(seller_zip_code_prefix AS TEXT) AS seller_zip_code_prefix,
                LOWER(TRIM(seller_city)) AS seller_city,
                UPPER(TRIM(seller_state)) AS seller_state
            FROM sellers;
        """)
        self.conn.execute("DROP TABLE sellers;")
        self.conn.execute("ALTER TABLE sellers_convert RENAME TO sellers;")
