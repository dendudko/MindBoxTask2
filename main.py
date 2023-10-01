import pyspark

spark_session = pyspark.sql.SparkSession.builder.getOrCreate()

products = spark_session.createDataFrame([(1, 'Молоко'), (2, 'Сыр'), (3, 'Пиво'), (4, 'Кофе'), (5, 'Нечто')],
                                         ['productID', 'productName'])

categories = spark_session.createDataFrame([(1, 'Молочные продукты'), (2, 'Алкоголь'),
                                            (3, 'Бакалея'), (4, 'Твердая молочка')],
                                           ['categoryID', 'categoryName'])

products_categories = spark_session.createDataFrame([(1, 1), (2, 1), (2, 4), (3, 2), (4, 3)],
                                                    ['productID', 'categoryID'])

products.createOrReplaceTempView('products')
categories.createOrReplaceTempView('categories')
products_categories.createOrReplaceTempView('products_categories')
products.show()
categories.show()
products_categories.show()

result = spark_session.sql('''
select p.productName, c.categoryName 
from products p
left join products_categories pc on pc.productID = p.productID
left join categories c on pc.categoryID = c.categoryID;''')

result.show()
