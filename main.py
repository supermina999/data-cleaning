import sqlite3
import pandas as pd
import os
import shutil

def init_db(data_folder):
    con = sqlite3.connect(':memory:')
    pd.read_csv(data_folder + '/Dish.csv').to_sql('Dish', con)
    pd.read_csv(data_folder + '/Menu.csv').to_sql('Menu', con)
    pd.read_csv(data_folder + '/MenuItem.csv').to_sql('MenuItem', con)
    pd.read_csv(data_folder + '/MenuPage.csv').to_sql('MenuPage', con)
    return con


def export_table(table_name, file_name, con):
    df = pd.read_sql_query('SELECT * FROM ' + table_name, con)
    if 'index' in df.columns:
        df = df.drop(columns='index')
    df.to_csv(file_name, index=False)


def export_db(result_folder, con):
    if os.path.exists(result_folder):
        shutil.rmtree(result_folder)
    os.mkdir(result_folder)
    export_table('DishAgg', result_folder + '/Dish.csv', con)
    export_table('Menu', result_folder + '/Menu.csv', con)
    export_table('MenuItemAgg', result_folder + '/MenuItem.csv', con)
    export_table('MenuPage', result_folder + '/MenuPage.csv', con)


def cleanup_missing_references(cursor):
    cursor.execute('''
            DELETE FROM MenuPage WHERE id in (
                SELECT MenuPage.id FROM MenuPage
                LEFT JOIN Menu on MenuPage.menu_id = Menu.id
                WHERE Menu.id is null
            )
        ''')
    cursor.execute('''
                DELETE FROM MenuItem WHERE id in (
                    SELECT MenuItem.id FROM MenuItem
                    LEFT JOIN MenuPage on MenuItem.menu_page_id = MenuPage.id
                    WHERE MenuPage.id is null
                )
            ''')
    cursor.execute('''
            DELETE FROM MenuItem WHERE id in (
                SELECT MenuItem.id FROM MenuItem
                LEFT JOIN Dish on MenuItem.dish_id = Dish.id
                WHERE Dish.id is null
            )
        ''')


def check_reference_icv(cursor):
    rows = cursor.execute('''
            SELECT MenuItem.id FROM MenuItem
            LEFT JOIN Dish on MenuItem.dish_id = Dish.id
            WHERE Dish.id is null
        ''').fetchall()
    assert len(rows) == 0
    rows = cursor.execute('''
                SELECT MenuItem.id FROM MenuItem
                LEFT JOIN MenuPage on MenuItem.menu_page_id = MenuPage.id
                WHERE MenuPage.id is null
            ''').fetchall()
    assert len(rows) == 0
    rows = cursor.execute('''
                SELECT MenuPage.id FROM MenuPage
                LEFT JOIN Menu on MenuPage.menu_id = Menu.id
                WHERE Menu.id is null
            ''').fetchall()
    assert len(rows) == 0


def fix_missing_dish_dates(cursor):
    cursor.execute('''
                UPDATE Dish
                SET first_appeared = null
                WHERE first_appeared = 0
            ''')
    cursor.execute('''
                 UPDATE Dish
                 SET last_appeared = null
                 WHERE last_appeared = 0
             ''')


def check_dish_dates_icv(cursor):
    rows = cursor.execute('''
                    SELECT * FROM Dish
                    WHERE first_appeared > last_appeared
                ''').fetchall()
    assert len(rows) == 0


def aggregate_dishes(cursor):
    cursor.execute('''
            CREATE TABLE DishAgg AS
            SELECT
                MIN(id) AS id,
                name,
                SUM(menus_appeared) AS menus_appeared,
                SUM(times_appeared) AS times_appeared,
                MIN(first_appeared) AS first_appeared,
                MAX(last_appeared) AS last_appeared,
                MIN(lowest_price) AS lowest_price,
                MAX(highest_price) AS highest_price
            FROM Dish
            GROUP BY name
        ''')
    cursor.execute('''
            CREATE TABLE DishMapping AS
            SELECT Dish.id AS id, DishAgg.id AS mid FROM Dish
            LEFT JOIN DishAgg on Dish.name = DishAgg.name
        ''')
    cursor.execute('''
            CREATE TABLE MenuItemAgg AS
            SELECT MenuItem.id as id, menu_page_id, price, high_price,
                DishMapping.mid as dish_id,
                created_at, updated_at, xpos, ypos
            FROM MenuItem
            LEFT JOIN DishMapping on MenuItem.dish_id = DishMapping.id
        ''')


if __name__ == '__main__':
    con = init_db('data')
    cursor = con.cursor()
    cleanup_missing_references(cursor)
    check_reference_icv(cursor)
    fix_missing_dish_dates(cursor)
    check_dish_dates_icv(cursor)

    aggregate_dishes(cursor)

    export_db('result', con)
