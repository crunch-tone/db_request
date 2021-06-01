import pyodbc
import csv
import glob
import pprint

data_to_compare_from_csv = [] #var for handling data parsed from csv files
pp = pprint.PrettyPrinter(indent=4)

def init():

    print("Let's start!")
        
    def parse_data_to_obj(parsedata):
        batches = []
        components_per_batch = {}
        parts_replacement_list = []
        rows_counter = 0
        batch_name = ''
        for row in parsedata:
            if rows_counter == 0:
                pass
            elif row[0]=='' and row[1]=='':
                break
            else:
                part_numbers = []
                for i in [row[4], row[5], row[6], row[7]]:
                    if i != '':
                        part_numbers.append(i)
                part_dict = {'PN':part_numbers,'Q':int(row[11])}
                if batch_name != row[0]:
                    batch_name = row[0]
                    
                    if components_per_batch != {}:
                        list_to_add = components_per_batch.copy()
                        batches.append(list_to_add)

                    parts_replacement_list = []
                    parts_replacement_list.append(part_dict)
                    components_per_batch.update(batch_name = batch_name, batch_q = int(row[1]), components = parts_replacement_list)
                        
                else:
                    parts_replacement_list.append(part_dict)
                    components_per_batch.update(components = parts_replacement_list)

            rows_counter+=1

        list_to_add = components_per_batch.copy()
        batches.append(list_to_add)

        return batches 

    def csv_parser(filename):
        with open(filename, newline='', encoding='utf-8') as csvfile:
            components_in_use_csv = csv.reader(csvfile, delimiter=';', quotechar='|')
            data_list = parse_data_to_obj(components_in_use_csv)
            if data_to_compare_from_csv == []:
                print("add")
                data_to_compare_from_csv.extend(data_list)
            else:
                print("else")
                for i_new in data_list:
                    counter=len(data_to_compare_from_csv)
                    
                    for j_old in data_to_compare_from_csv:
                        
                        if i_new['batch_name'] == j_old['batch_name']:
                            old_batch_q = j_old['batch_q']
                            add_batch_q = i_new['batch_q']
                            j_old.update(batch_q=old_batch_q+add_batch_q)
                            for k_new in i_new['components']:
                                for l_old in j_old['components']:
                                    if any(x in k_new['PN'] for x in l_old['PN']):
                                        old_quantity = l_old['Q']
                                        add_quantity = k_new['Q']
                                        l_old.update(Q=old_quantity+add_quantity)

                        else:
                            counter-=1
                        
                    if counter == 0:
                        data_to_compare_from_csv.append(i_new)


    csv_files = glob.glob('*.csv')


    for file in csv_files:
        csv_parser(file)
        #try:
        #    csv_parser(file)
        #except:
        #    print("There is no *.csv files in the working directory")

    #pp.pprint(data_to_compare_from_csv)

    def db_request(parts_replacement_list, ref_quantity, batch):

        #connecting to DB
        conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=10.9.0.102\\flxdb;'
                            'Database=FLxDB2016;'
                            'UID=a22349;'
                            'PWD=gfhjkm22349;')

        #creating instance for query
        cursor = conn.cursor()

        def query(part_num, batch):
            #part_num_quantity = 0
            if part_num != '':
                cursor.execute("SELECT SUM(Placements) AS Placements FROM dbo.MaterialConsumptions WHERE BatchName = '"+batch['batch_name']+"' and PartNumber = '"+part_num+"';")
                result_row = cursor.fetchone()
                #print("SELECT SUM(Placements) AS Placements FROM dbo.MaterialConsumptions WHERE BatchName = '"+batch['batch_name']+"' and PartNumber = '"+part_num+"';")
                
                try:
                    part_num_quantity = int(result_row[0])
                except:
                    part_num_quantity = 0
            return part_num_quantity

        parts_replacement_quantity = 0

        for part in parts_replacement_list:
            parts_replacement_quantity += query(part, batch)


        if parts_replacement_quantity != ref_quantity:
            result_ng.append((parts_replacement_list, "delta is = "+str(ref_quantity-parts_replacement_quantity)))
        else:
            result_ok.append((parts_replacement_list, "OK"))
        
    for item in data_to_compare_from_csv:

        result_ok = []
        result_ng = []
        for i in item['components']:
            parts = i['PN']
            part_quantity_csv = i['Q']

            db_request(parts, part_quantity_csv, item)

        with open('result.txt', 'a') as f:

            if len(result_ng)==0:
                print("Batch "+item['batch_name']+":\n Both lists are fully matched", file=f)
            
            else:
                print("Batch "+item['batch_name']+" good results are:", file=f)
                print(result_ok, file=f)
                print("Batch "+item['batch_name']+" different results are:", file=f)
                print(result_ng, file=f)

    print("Completed. Results saved into the file result.txt")

init()