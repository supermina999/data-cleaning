import sqlite3
import pandas as pd
import os
import shutil

# @begin data_cleaning_workflow
# @in input_dish.csv @uri file:data/Dish.csv
# @in input_menu.csv @uri file:data/Menu.csv
# @in input_menu_item.csv @uri file:data/MenuItem.csv
# @in input_menu_page.csv @uri file:data/MenuPage.csv
# @out output_dish.csv @uri file:result/Dish.csv
# @out output_menu.csv @uri file:result/Menu.csv
# @out output_menu_item.csv @uri file:result/MenuItem.csv
# @out output_menu_page.csv @uri file:result/MenuPage.csv

# @begin dish_OpenRefine_cleaning
# @in input_dish.csv
# @out OR_cleaned_dish.csv @uri file:OR_cleaned/Dish.csv
# @end dish_OpenRefine_cleaning

# @begin menupage_OpenRefine_cleaning
# @in input_menu_page.csv
# @out OR_cleaned_menu_page.csv @uri file:OR_cleaned/MenuPage.csv
# @end menupage_OpenRefine_cleaning

# @begin menuitem_OpenRefine_cleaning
# @in input_menu_item.csv
# @out OR_cleaned_menu_item.csv @uri file:OR_cleaned/MenuItem.csv
# @end menuitem_OpenRefine_cleaning

# @begin menu_OpenRefine_cleaning
# @in input_menu.csv
# @out OR_cleaned_menu.csv @uri file:OR_cleaned/Menu.csv
# @end menu_OpenRefine_cleaning


# @begin sqlite_import
# @in OR_cleaned_dish.csv
# @in OR_cleaned_menu.csv
# @in OR_cleaned_menu_item.csv
# @in OR_cleaned_menu_page.csv
# @out dish
# @out menu
# @out menu_item
# @out menu_page
def init_db(data_folder):
    con = sqlite3.connect(':memory:')
    pd.read_csv(data_folder + '/Dish.csv').to_sql('Dish', con)
    pd.read_csv(data_folder + '/Menu.csv').to_sql('Menu', con)
    pd.read_csv(data_folder + '/MenuItem.csv').to_sql('MenuItem', con)
    pd.read_csv(data_folder + '/MenuPage.csv').to_sql('MenuPage', con)
    return con
# @end sqlite_import

def export_table(table_name, file_name, con):
    df = pd.read_sql_query('SELECT * FROM ' + table_name, con)
    if 'index' in df.columns:
        df = df.drop(columns='index')
    df.to_csv(file_name, index=False)


# @begin sqlite_export
# @in dish_agg
# @in menu
# @in menu_item_agg
# @in menu_page_v2
# @out output_dish.csv @uri file:result/Dish.csv
# @out output_menu.csv @uri file:result/Menu.csv
# @out output_menu_item.csv @uri file:result/MenuItem.csv
# @out output_menu_page.csv @uri file:result/MenuPage.csv
def export_db(result_folder, con):
    if os.path.exists(result_folder):
        shutil.rmtree(result_folder)
    os.mkdir(result_folder)

    export_table('DishAgg', result_folder + '/Dish.csv', con)
    export_table('Menu', result_folder + '/Menu.csv', con)
    export_table('MenuItemAgg', result_folder + '/MenuItem.csv', con)
    export_table('MenuPage', result_folder + '/MenuPage.csv', con)
# @end sqlite_export


def cleanup_missing_references(cursor):
    # @begin remove_invalid_menu_pages
    # @in menu_page
    # @in menu
    # @out menu_page_v2
    cursor.execute('''
            DELETE FROM MenuPage WHERE id in (
                SELECT MenuPage.id FROM MenuPage
                LEFT JOIN Menu on MenuPage.menu_id = Menu.id
                WHERE Menu.id is null
            )
        ''')
    # @end remove_invalid_menu_pages

    # @begin remove_invalid_menu_items
    # @in menu_item
    # @in dish
    # @in menu_page_v2
    # @out menu_item_v2
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
    # @end remove_invalid_menu_items


def check_reference_icv(cursor):
    # @begin check_menu_item_icv
    # @in menu_item_v2
    # @in dish
    # @in menu_page_v2
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
    # @end check_menu_item_icv

    # @begin check_menu_page_icv
    # @in menu_page_v2
    # @in menu
    rows = cursor.execute('''
                SELECT MenuPage.id FROM MenuPage
                LEFT JOIN Menu on MenuPage.menu_id = Menu.id
                WHERE Menu.id is null
            ''').fetchall()
    assert len(rows) == 0
    # @end check_menu_page_icv


def fix_missing_dish_dates(cursor):
    # @begin fix_dish_missing_dates
    # @in dish
    # @out dish_v2
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
    # @end fix_dish_missing_dates


def check_dish_dates_icv(cursor):
    # @begin check_dish_dates_icv
    # @in dish_v2
    rows = cursor.execute('''
                    SELECT * FROM Dish
                    WHERE first_appeared > last_appeared
                ''').fetchall()
    assert len(rows) == 0
    # @end check_dish_dates_icv


def aggregate_dishes(cursor):
    # @begin aggregate_dishes
    # @in dish_v2
    # @out dish_agg
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
    # @end aggregate_dishes

    # @begin create_dish_mapping
    # @in dish_v2
    # @in dish_agg
    # @out dish_mapping
    cursor.execute('''
            CREATE TABLE DishMapping AS
            SELECT Dish.id AS id, DishAgg.id AS mid FROM Dish
            LEFT JOIN DishAgg on Dish.name = DishAgg.name
        ''')
    # @end create_dish_mapping

    # @begin aggregate_menu_items
    # @in menu_item_v2
    # @in dish_mapping
    # @out menu_item_agg
    cursor.execute('''
            CREATE TABLE MenuItemAgg AS
            SELECT MenuItem.id as id, menu_page_id, price, high_price,
                DishMapping.mid as dish_id,
                created_at, updated_at, xpos, ypos
            FROM MenuItem
            LEFT JOIN DishMapping on MenuItem.dish_id = DishMapping.id
        ''')
    # @end aggregate_menu_items

# @end data_cleaning_workflow

if __name__ == '__main__':
    con = init_db('OR_cleaned')
    cursor = con.cursor()
    cleanup_missing_references(cursor)
    check_reference_icv(cursor)
    fix_missing_dish_dates(cursor)
    check_dish_dates_icv(cursor)

    aggregate_dishes(cursor)

    export_db('result', con)
