import sys
import getopt
import db
import simplejson as json
import config
import psycopg2
import nassusda

def begin_nass_harvest(database_host, database_name, database_user, database_password,
                       port, start_date, end_date):
    print "\nScript for Gro Hackathon's NASS harvest." # " \
          # "requirements defined for the hackathon\n\n"

    print "Run 'python harvest.py -h' for help\n\n"
    # print "Feel free to edit the entirety of this start script\n"

    # print "Supplied Args (some default): "
    # print "Database Host: {}".format(database_host)
    # print "Database Name: {}".format(database_name)
    # print "Database Username: {}".format(database_user)
    # print "Database Password: {}".format(database_password)
    # print "Database Port (hard-coded): {}".format(port)
    # print "Harvest Start Date: {}".format(start_date)
    # print "Harvest End Date: {}\n".format(end_date)

    try:
        # setup connection for PostgreSQL
        conn = db.get_connection(database_host, database_name, database_user, database_password)
        # conn = psycopg2.connect("dbname='"+database_name+"'user='"+database_user+"'password='"+database_password+"'host='" + database_host +"'")

        cursor = conn.cursor()

        table_fact_data = 'CREATE TABLE IF NOT EXISTS fact_data ('
        table_fact_data += 'DOMAIN_DESC varchar NOT NULL, '
        table_fact_data += 'COMMODITY_DESC varchar NOT NULL, '
        table_fact_data += 'STATISTICCAT_DESC varchar NOT NULL, '
        table_fact_data += 'AGG_LEVEL_DESC varchar NOT NULL, '
        table_fact_data += 'COUNTRY_NAME varchar NOT NULL, '
        table_fact_data += 'STATE_NAME varchar NOT NULL, '
        table_fact_data += 'COUNTY_NAME varchar NOT NULL, '
        table_fact_data += 'UNIT_DESC varchar NOT NULL, '
        table_fact_data += 'VALUE varchar NOT NULL, '
        table_fact_data += 'YEAR varchar NOT NULL'
        table_fact_data += ');'
        table_fact_data += 'TRUNCATE TABLE fact_data;'

        # Create table
        #print table_fact_data
        cursor.execute(table_fact_data) # Save changes to database db.commit()

        conn.commit()
        
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception("Error: {}".format(3))

    finally:

        parse_nass(database_host, database_name, database_user, database_password, start_date, end_date)



def parse_nass(database_host, database_name, database_user, database_password, start_date = "2014-01-01", end_date= "2015-01-01"):
    # rest.get_param_values("sector_desc")
    api = nassusda.USDAApi(config.API_KEY)

    # print json.dumps(api.param_values('source_desc'), sort_keys = False, indent = 4)

    # store the data groups
    groups = api.param_values('group_desc')
    states = api.param_values('state_name')
    # print json.dumps(groups, sort_keys = False, indent = 4)
    # print json.dumps(states, sort_keys = False, indent = 4)



    q = api.query()
    # filter CROP SECTOR to the COUNTY LEVEL for the start date and end date
    q.filter('sector_desc', 'CROPS').filter('agg_level_desc', 'COUNTY').filter('year', start_date, 'ge').filter('year', end_date, 'le')

    print 'Number of Records: {}'.format(q.count())

    if q.count() > 0 and q.count() <= 50000:
        # print json.dumps(q.execute(), sort_keys = False, indent = 4)
        save_results(database_host, database_name, database_user, database_password, q.execute())

    else:
        for group in groups:
            q.filter('sector_desc', 'CROPS').filter('agg_level_desc', 'COUNTY').filter('year', start_date, 'ge').filter('year', end_date, 'le')
            print 'Number of Records in {}: {}'.format(group, q.count())
            if q.count() > 0 and q.count() <= 50000:
                      save_results(database_host, database_name, database_user, database_password, q.execute())
            else:
                for state in states:
                    q.filter('sector_desc', 'CROPS').filter('agg_level_desc', 'COUNTY').filter('state_name', state).filter('year', start_date, 'ge').filter('year', end_date, 'le')
                    # print 'Number of Records in {}: {}'.format(state, q.count()) 
                    save_results(database_host, database_name, database_user, database_password, q.execute



def save_results(database_host, database_name, database_user, database_password, data):
    """
    Function to save data to a database table called 'fact_data'
    """

    temp_data = []
    for row in data:
        domain = row['domain_desc']
        commodity = row['commodity_desc']
        category = row['statisticcat_desc']
        geography = row['agg_level_desc']
        country = row['country_name']
        state = row['state_name']
        county = row['county_name']
        description = row['unit_desc']
        value = row['Value']
        year = row['year']
        temp_data.append([domain, commodity, category, geography, country, state, county, description, value, year])
        
    # Convert the list to a tuple
    temp_data = tuple(temp_data)
        
    try:
        # get database connection
        conn = db.get_connection(database_host, database_name, database_user, database_password)

        cur = conn.cursor()
        query = "INSERT INTO fact_data (domain_desc, commodity_desc, statisticcat_desc, agg_level_desc, country_name, state_name, county_name, unit_desc, value, year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cur.executemany(query, temp_data)
            
        conn.commit()
    except psycopg2.DatabaseError as e:
        if conn:
            conn.rollback()
        raise Exception("Error: {}".format(3))


# #################################################
# PUT YOUR CODE ABOVE THIS LINE
# #################################################
def main(argv):
    try:
        opts, args = getopt.getopt(argv, "h", ["database_host=", "database_name=", "start_date=",
                                               "database_user=", "database_pass=", "end_date="])
    except getopt.GetoptError:
        print 'Flag error. Probably a mis-typed flag. Make sure they start with "--". Run python ' \
              'harvest.py -h'
        sys.exit(2)

    #define defaults
    database_host = 'localhost'
    database_name = 'gro'
    port = 5432
    database_user = 'postgres'
    database_password = 'bandit'
    start_date = '2012-01-01'
    end_date = '2015-12-31'

    for opt, arg in opts:
        if opt == '-h':
            print "\nThis is my harvest script for the Gro Hackathon NASS harvest"
            print '\nExample:\npython harvest.py --database_host localhost --database_name gro2\n'
            print '\nFlags (all optional, see defaults below):\n ' \
              '--database_host [default is "{}"]\n ' \
              '--database_name [default is "{}"]\n ' \
              '--database_user [default is "{}"]\n ' \
              '--database_pass [default is "{}"]\n ' \
              '--start_date [default is "{}"]\n ' \
              '--end_date [default is "{}"]\n'.format(database_host, database_name, database_user,
                                                      database_password, start_date, end_date)
            sys.exit()
        elif opt in ("--database_host"):
            database_host = arg
        elif opt in ("--database_name"):
            database_name = arg
        elif opt in ("--database_user"):
            database_user = arg
        elif opt in ("--database_pass"):
            database_password = arg
        elif opt in ("--start_date"):
            start_date = arg
        elif opt in ("--end_date"):
            end_date = arg

    begin_nass_harvest(database_host, database_name, database_user, database_password,
                       port, start_date, end_date)

if __name__ == "__main__":
   main(sys.argv[1:])