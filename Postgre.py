import psycopg2

def create_poly(x1, y1, x2, y2):
    poly = 'POLYGON (({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))'.format(x1, y1, x2, y2)
    return poly

def create():
    conn = psycopg2.connect(database="postgres", user="postgres",
                        password="123456", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE polygontable ( id SERIAL PRIMARY KEY,  name VARCHAR(128), geom GEOMETRY(POLYGON, 3857));")
    conn.commit()
    cursor.close()
    conn.close()

def insert(name, x1, y1, x2, y2):
    poly = create_poly(x1, y1, x2, y2)
    
    conn = psycopg2.connect(database="postgres", user="postgres",
                        password="123456", host="127.0.0.1", port="5432")
    cursor = conn.cursor()

    cursor.execute("INSERT INTO polygontable (name, geom) VALUES ('{0}',  ST_GeomFromText('{1}', 3857));".format(name, poly))

    conn.commit()
    cursor.close()
    conn.close()
    
def select(x1, y1, x2, y2):
    poly = create_poly(x1, y1, x2, y2)
    conn = psycopg2.connect(database="postgres", user="postgres",
                    password="123456", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    
    cursor.execute("select name from polygontable t where ST_Intersects(t.geom,ST_GeomFromText('{0}', 3857));".format(poly))

    rows = cursor.fetchall()
    for row in rows:
        print('name = ', row[0])

    conn.commit()
    cursor.close()
    conn.close()
    
    return len(list(rows))