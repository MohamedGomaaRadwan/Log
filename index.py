import psycopg2


query_1 = """create or replace view popular_articles as
             select title, count(title) as num from articles join
             log on log.path = concat('/article/', articles.slug)
             group by title order by num desc limit 3;
             select * from popular_articles;
          """
query_2 = """ create or replace view top_authors as
              select name, count(name) as num from articles join
              authors on articles.author = authors.id join
              log on log.path = concat('/article/', articles.slug)
              group by name
              order by num desc limit 4;
              select * from top_authors;
          """
query_3 = """create or replace view error_percent as
             select main.date,(100.0*main.error/main.num)
             from (SELECT date_trunc('day', time) as date,
             count(id) as num,
             sum(case when status='404 NOT FOUND' then 1 else 0 end)
             as error
             from log
             group by date) as main
             where (100.0*main.error/main.num) >1;
             select * from error_percent;
          """
# open output file
f = open('output.txt', 'w')


# Connection Function
def connect(conn="news"):
    try:
        # to return connection object
        conn = psycopg2.connect(database=conn)
        # to run queries and fetch result
        cursor = conn.cursor()
        # connect
        return conn, cursor
    except Exception:
        # if DB not exist
        print("can't connect to the database...")


# Function which execute SQL queries
def excuite_queries(query):

    conn, cursor = connect()
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()
    conn.close()

# i make three function to display each query alone
# because it's diffrent out put for each query


# Function which execute query 1
# and store the result in output file
def query_1_result(query):

    # Execute the first query
    results = excuite_queries(query)
    # to write title in the file
    f.write("\n The most three articles : \n \n")
    # for loop to write results in the file
    for title, views in results:
        f.write("\t" + title + "--" + str(views) + " views \n")


def query_2_result(query):
    # Execute the second query
    results = excuite_queries(query)
    # to write title in the file
    f.write("\n The top authors : \n \n")
    # for loop to write results in the file
    for author, views in results:
        f.write("\t" + author + "--"+str(views) + " views \n")


def query_3_result(query):
    # Execute the third query
    results = excuite_queries(query)
    # to write title in the file
    f.write("\n Error percent more than 1 % :\n \n")
    # for loop to write results in the file
    for date, error in results:
        #
        f.write("\t {0:%B %d, %Y} -- {1:.2f} % errors".format(date, error))


if __name__ == '__main__':
    query_1_result(query_1)
    query_2_result(query_2)
    query_3_result(query_3)
    # to close the file
    f.close()
