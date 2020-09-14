
# encoding = utf-8

import os
import sys
import time
import datetime
import random as r
import csv
'''
    IMPORTANT
    Edit only the validate_input and collect_events functions.
    Do not edit any other part in this file.
    This file is generated only once when creating the modular input.
'''

app_location = os.path.dirname(os.path.dirname(__file__))

def validate_input(helper, definition):
    """Implement your own validation logic to validate the input stanza configurations"""
    # This example accesses the modular input variable
    # number_of_orders_per_day = definition.parameters.get('number_of_orders_per_day', None)
    # std_dev_number_of_orders = definition.parameters.get('std_dev_number_of_orders', None)
    pass

""" Start of the data gen code""" """"""
def collect_events(helper, ew):
    
    """Open all the lookups and save them as lists"""
    with open(os.path.join(app_location, 'lookups', 'clients.csv')) as csvfile:
        df = csv.reader(csvfile, delimiter=';')
        clients = [list(row) for row in df][1:]
    with open(os.path.join(app_location, 'lookups', 'product_lookup.csv')) as csvfile:
        df = csv.reader(csvfile, delimiter=';')
        products = [list(row) for row in df][1:]
    with open(os.path.join(app_location, 'lookups', 'suppliers.csv')) as csvfile:
        df = csv.reader(csvfile, delimiter=';')
        suppliers = [list(row) for row in df][1:]
    with open(os.path.join(app_location, 'lookups', 'depots.csv')) as csvfile:
        df = csv.reader(csvfile, delimiter=';')
        depots = [list(row) for row in df][1:]
    with open(os.path.join(app_location, 'lookups', 'crew.csv')) as csvfile:
        df = csv.reader(csvfile, delimiter=';')
        crew = [list(row) for row in df][1:]
    with open(os.path.join(app_location, 'lookups', 'recurrent_orders.csv')) as csvfile:
        df = csv.reader(csvfile, delimiter=';')
        recurrent_orders = [list(row) for row in df][1:]
        
    """Order counter keeps track of the last order ID, such that on new days it
    will start from this number"""
    if os.path.exists(os.path.join(app_location, 'lookups', 'order_counter.csv')):
        with open(os.path.join(app_location, 'lookups', 'order_counter.csv')) as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            order_counter = int([list(row) for row in df][0][0])

    else:
        order_counter = 0

    """Get todays and yesterdays date"""
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    
    orders_log1 = []
    event_list = []
    
    date = datetime.datetime.now()
    starthourinseconds = 10*60*60
    
    """ 
    If starting from an earlier date, the data which is located in date.csv
    will use to create data from previous days. This updates the today variable
    from before
    """
    with open(os.path.join(app_location, 'order_data', 'date.csv')) as csvfile:
        df = csv.reader(csvfile, delimiter=',')
        dates = [list(row) for row in df]
        today = dates[0][0]
    
    """ create a new date variable based of the date.csv, will be used for event
    creation"""
    date_csv = datetime.datetime(int(today[6:8])+2000, int(today[:2]), int(today[3:5]))
    
    """ Convert the time now in to seconds, is used to determine when events are released into splunk """
    def now_to_seconds(date):
        todays_time_in_seconds = int(date.hour) * 60 * 60 + int(date.minute) * 60 + int(date.second) + 7200
        return todays_time_in_seconds
    
    """ seconds can then also be sent back into hours:minutes:seconds and used
    in the event creation for time, the date from date.csv is added to time"""
    def convert(seconds):
        seconds = seconds % (24 * 3600)
        hour = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60

        return date_csv.strftime("%x")+" " + "%d:%02d:%02d" % (hour, minutes, seconds)

    """ Each time the code is run one of two things can happend. If the event data
    is already created it will run if statement below. This slowly releases the
    saved events to splunk. The elif statement below this if statement only
    creates new data when it is a new day and the event data does not exist. 
    This happens after all data is released to splunk. At this 
    point the code deletes all saved event files (also known as 
    order_data_for_today.csv files) """
    
    if os.path.exists(os.path.join(app_location, 'order_data', 'order_data_for_today1.csv')):
        """ If the days match, open the different order files. The order files 
        are saved seperatly due to the fact that not all events are the same. 
        Some events have supplier_id, crew_id, others don't. Therefor they 
        needed to be saved seperatly."""
        if today <= datetime.date.today().strftime('%m/%d/%y'):
            fieldname_collect = []
            with open(os.path.join(app_location, 'order_data', 'order_data_for_today1.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                all = [list(row) for row in df]
                fieldnames = all[0]
                for x in range(len(all[1:])):
                    fieldname_collect.append(fieldnames)
                event_data = all[1:]
            with open(os.path.join(app_location, 'order_data', 'order_data_for_today2.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                all = [list(row) for row in df]
                fieldnames = all[0]
                for x in range(len(all[1:])):
                    fieldname_collect.append(fieldnames)
                event_data.extend(all[1:])
            with open(os.path.join(app_location, 'order_data', 'order_data_for_today3.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                all = [list(row) for row in df]
                fieldnames = all[0]
                for x in range(len(all[1:])):
                    fieldname_collect.append(fieldnames)
                event_data.extend(all[1:])
            with open(os.path.join(app_location, 'order_data', 'order_data_for_today4.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                all = [list(row) for row in df]
                fieldnames = all[0]
                for x in range(len(all[1:])):
                    fieldname_collect.append(fieldnames)
                event_data.extend(all[1:])
            with open(os.path.join(app_location, 'order_data', 'order_data_for_today5.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                all = [list(row) for row in df]
                fieldnames = all[0]
                for x in range(len(all[1:])):
                    fieldname_collect.append(fieldnames)
                event_data.extend(all[1:])
            with open(os.path.join(app_location, 'order_data', 'order_data_for_today6.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                all = [list(row) for row in df]
                fieldnames = all[0]
                for x in range(len(all[1:])):
                    fieldname_collect.append(fieldnames)
                event_data.extend(all[1:])
            with open(os.path.join(app_location, 'order_data', 'order_data_for_today7.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                all = [list(row) for row in df]
                fieldnames = all[0]
                for x in range(len(all[1:])):
                    fieldname_collect.append(fieldnames)
                event_data.extend(all[1:])
            
            """Order Datetime are list which match the order data lists but only
            hold the time in seconds format. By using these list we can sort 
            them and return the indexes of the order data which can be released
            based on time"""
            
            with open(os.path.join(app_location, 'order_data', 'order_datatime_for_today1.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                time_data = [list(row) for row in df]
            with open(os.path.join(app_location, 'order_data', 'order_datatime_for_today2.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                time_data.extend([list(row) for row in df])
            with open(os.path.join(app_location, 'order_data', 'order_datatime_for_today3.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                time_data.extend([list(row) for row in df])
            with open(os.path.join(app_location, 'order_data', 'order_datatime_for_today4.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                time_data.extend([list(row) for row in df])
            with open(os.path.join(app_location, 'order_data', 'order_datatime_for_today5.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                time_data.extend([list(row) for row in df])
            with open(os.path.join(app_location, 'order_data', 'order_datatime_for_today6.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                time_data.extend([list(row) for row in df])
            with open(os.path.join(app_location, 'order_data', 'order_datatime_for_today7.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                time_data.extend([list(row) for row in df])
            
            """Row is used to skip events which already have been released to
            splunk and therefor prevents duplicates"""
            
            with open(os.path.join(app_location, 'order_data', 'row.csv')) as csvfile:
                df = csv.reader(csvfile, delimiter=',')
                row = [list(row) for row in df][0][0]
            
            """ Sort the timedata list and return a list if indexes which show
            which items in the order data list are to be released first"""
            sorted_time_list_indexes = sorted(range(len(time_data)), key=lambda k: time_data[k])
            sorted_time_list = sorted(time_data, key=lambda x: x[0])
    
            time_now = now_to_seconds(date)
            row = int(row)
            
            
            """ Release the events based on time and the ordered time list"""
            for x in range(len(sorted_time_list)):
                """ If all data has been released to splunk, remove the order
                data files"""
                if row == len(event_data)-1:
                    for file in os.listdir(os.path.join(app_location, 'order_data')):
                        if file == "date.csv":
                            mycsv = csv.writer(
                                open(os.path.join(app_location, 'order_data', 'date.csv'), 'w',
                                     newline=''))
                            mycsv.writerows([[(date_csv + datetime.timedelta(days=1)).strftime('%m/%d/%y')]])
    
                            continue
                        os.remove(os.path.join(app_location, 'order_data', str(file)))
    
                    exit()
                
                """ If the order data has been released already, skip it"""
                if x < row:
                    continue
                
                """ Else, release the order data as an event"""
                if int(float(sorted_time_list[x][0])) < time_now or today < datetime.date.today().strftime('%m/%d/%y'):
                    log = event_data[sorted_time_list_indexes[x]]
                    names = fieldname_collect[sorted_time_list_indexes[x]]

                    e = ""
    
                    for y in range(len(names)):
                        if e != "":
                            e = e+", "
                        e = e + str(names[y]) + "; " + str(log[y]) 
    
                    event = helper.new_event(str(e), time=None, host=None, index=None, source=None, sourcetype=None,
                                         done=True, unbroken=True)
                    ew.write_event(event)

    
                else:
                    break
            """Update the row.csv file"""
            mycsv = csv.writer(
                open(os.path.join(app_location, 'order_data', 'row.csv'), 'w',
                     newline=''))
            mycsv.writerows([[x]])
    
    elif today <= datetime.date.today().strftime('%m/%d/%y'):
        """Get a random number of new orders"""
        opt_number_of_orders_per_day = 750
        opt_std_dev_number_of_orders = 250
        num_new_orders = r.randrange(500, 1000)
    
    
        """
        Create a random probablity for the orders.
        In this case, the products in left will occur more often than right
        """
        p = 10000
        product_probs = []
        shuffle = []
        for x in range(100):
            product_probs.append(x+1)
            shuffle.append(x+1)
    
        r.shuffle(shuffle)
        left = shuffle[:50]
        right = shuffle[50:]
        for x in range(6600):
            product_probs.append(left[r.randrange(0, 50)])
        for x in range(3300):
            product_probs.append(right[r.randrange(0, 50)])

    
        orders_log1 = []
        event_list1, event_list2, event_list3, event_list4, event_list5, event_list6, event_list7 = [], [], [], [], [], [], []
        time_list1, time_list2, time_list3, time_list4, time_list5, time_list6, time_list7 = [], [], [], [], [], [], []
    
        starthourinseconds = 10*60*60

        """Add the recurrent orders to the new orders"""
        
        time = starthourinseconds
        day_of_the_week = date_csv.strftime('%A')
        for x in recurrent_orders:
            if x[0] == day_of_the_week:
                clientid = int(x[1])-10900
                product_id = int(x[2])-30000
                orders_log1.append([order_counter, clientid, product_id, products[product_id - 1][6], time])
                event_list1.append(
                    {"time": convert(time), "status": "Order Received from Client", "order_id": order_counter,
                     "client_id": clientid+10900, "product_id": product_id + 30000,
                     "supplier_id": products[product_id - 1][6]})
                time_list1.append([time])
                order_counter += 1
        
        """Create new random orders"""
        for n in range(num_new_orders):
            
            product_id = product_probs[r.sample(range(10000), 1)[0]]
            clientid = r.randrange(1, 281)
            orders_log1.append([order_counter, clientid, product_id, products[product_id-1][6], time])
            event_list1.append({"time": convert(time), "status": "Order Received from Client", "order_id": order_counter, "client_id": clientid+10900, "product_id": product_id+30000, "supplier_id": products[product_id-1][6]})
            time_list1.append([time])
    
            order_counter += 1
        total_orders = len(orders_log1)
    
        """Open csv's with the distance matrices"""
        with open(os.path.join(app_location, 'lookups', 'nearest_depots_from_suppliers.csv')) as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            nearest_depots = [list(row) for row in df]
    
    
        with open(os.path.join(app_location, 'lookups', 'depot_to_depot_distance.csv')) as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            depot_to_depot_distance = [list(row) for row in df]
    
    
        orders_log2 = []
    
        for order in orders_log1:
            supplierid = order[3]
            depotid = nearest_depots[int(supplierid)-101][0]
            
            
            """Add a random amount of time for the product to be ordered
            and sent to the bus in the depot"""
            time = starthourinseconds+r.randrange(600, 1800)
    
            event_list2.append(
                {"time": convert(time), "status": "Product in Transit to Depot", "order_id": order[0], "client_id": order[1]+10900, "product_id": order[2]+30000,
                 "supplier_id": supplierid, "depot_id": depotid})
            time_list2.append([time])
    
            """Depending on the distance to the nearest depot, extra time will
            be added to the time variable. This is to simulate the time it takes
            for the package to reach the nearest depot"""
            
            depotid = nearest_depots[int(supplierid) - 101][0]
            distance = nearest_depots[int(supplierid) - 101][1]
            time = time + float(distance)/60*3600
    
            event_list3.append(
                {"time": convert(time), "status": "Product Arrived at Nearest depot", "order_id": order[0], "client_id": order[1]+10900, "product_id": order[2]+30000,
                 "supplier_id": supplierid, "depot_id": depotid})
            time_list3.append([time])
    
            orders_log2.append([order[0], order[1], order[2], order[3], depotid, time])
    
        with open(os.path.join(app_location, 'lookups', 'nearest_depots_to_clients.csv')) as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            depot_to_clients_distance = [list(row) for row in df]
    
    
        count = 0
        d = 0
        crew_list_by_depot = [[] for _ in range(10)]
        ordercheck1 = [0 for _ in range(10)]
        ordercheck2 = [0 for _ in range(10)]
    
    
    
        for c in crew:
            crew_id = c[0]
            depotid = int(c[1]) - 201
            seniority = c[2]
            crew_list_by_depot[depotid].append([crew_id, seniority])
    
    
    
        orders_log3 = []
        for order in orders_log2:
            clientid = order[1]
            closest_clients = [float(i) for i in depot_to_clients_distance[clientid-1]]
            i = closest_clients.index(min(closest_clients))
            depotid_send_to = depots[i][0]
            depotid_current = order[4]
    
            time = order[5]
            
            """If the nearest depot is also the final depot, a crew member will
            bring the package to the last bus which will deliver the package
            to the client"""
            if depotid_send_to == depotid_current:
                depotid_to = int(depotid_send_to) - 201
                crew_size = len(crew_list_by_depot[depotid_to])
                crew_choice = r.randrange(0,crew_size)
                crew_id = crew_list_by_depot[depotid_to][crew_choice][0]
                crew_seniority = crew_list_by_depot[depotid_to][crew_choice][1]
    
                event_list4.append(
                    {"time": convert(time), "status": "Product Arrived at Final Depot", "order_id": order[0], "client_id": order[1]+10900, "product_id": order[2]+30000,
                     "depot_id": depotid_current, "crew_id": crew_id})
                time_list4.append([time])
                
                """Depending on the crew member seniority, he or she can lose 
                the package. This results in no more further status updates"""
                if r.randrange(11) > int(crew_seniority):
                    continue
                sec_to_bus1_to_bus2 = r.randrange(600, 1800)
                time = time + sec_to_bus1_to_bus2
                
                event_list5.append(
                    {"time": convert(time), "status": "Product Ready for Client Delivery", "order_id": order[0], "client_id": order[1] + 10900,
                     "product_id": order[2] + 30000,
                     "depot_id": depotid_current})
                time_list5.append([time])
    
                ordercheck1[depotid_to] += 1
    
                orders_log3.append([order[0], order[1], order[2], order[3], order[4]])
                
                
            
            else:
                """If the nearest depot is not the final depot, a crew member will
                bring the package to a bus which will deliver the package
                to a depot nearest to the client"""
                depotid_from = int(depotid_current) - 201
                depotid_to = int(depotid_send_to) - 201
    
                crew_size = len(crew_list_by_depot[depotid_from])
                crew_choice_from = r.randrange(0, crew_size)
                crew_id_from = crew_list_by_depot[depotid_from][crew_choice_from][0]
                crew_seniority_from = crew_list_by_depot[depotid_from][crew_choice_from][1]
    
                crew_size = len(crew_list_by_depot[depotid_to])
                crew_choice_to = r.randrange(0, crew_size)
                crew_id_to = crew_list_by_depot[depotid_to][crew_choice_to][0]
                crew_seniority_to = crew_list_by_depot[depotid_to][crew_choice_to][1]
    
    
                sec_to_bus1_to_bus2 = r.randrange(600, 1800)
                time = time + sec_to_bus1_to_bus2
    
    
                event_list6.append(
                    {"time": convert(time), "status": "Product in Transit to Final Depot", "order_id": order[0], "client_id": order[1]+10900,
                     "product_id": order[2]+30000,
                     "depot_id": depotid_current, "crew_id": crew_id_from})
                time_list6.append([time])
                
                
                """Depending on the crew member seniority, he or she can lose 
                the package. This results in no more further status updates"""
    
                if r.randrange(11) > int(crew_seniority_from):
                    continue
    
                distance = depot_to_depot_distance[depotid_from][depotid_to]
                time = time + float(distance)/60*3600
                
                """Once at the final depot, a crew member will
                 bring the package to a bus which will deliver the package
                 to the clients"""
                 
                event_list4.append(
                    {"time": convert(time), "status": "Product Arrived at Final Depot", "order_id": order[0], "client_id": order[1]+10900,
                     "product_id": order[2]+30000,
                     "depot_id": depotid_send_to, "crew_id": crew_id_to})
                time_list4.append([time])
                
                """Depending on the crew member seniority, he or she can lose 
                the package. This results in no more further status updates"""
                
                if r.randrange(11) > int(crew_seniority_to):
                    continue
    
                sec_to_bus1_to_bus2 = r.randrange(600, 1800)
                time = time + sec_to_bus1_to_bus2
    
                event_list5.append(
                    {"time": convert(time), "status": "Product Ready for Client Delivery", "order_id": order[0], "client_id": order[1] + 10900,
                     "product_id": order[2] + 30000,
                     "depot_id": depotid_send_to})
                time_list5.append([time])
    
                ordercheck1[depotid_to] += 1
    
                orders_log3.append([order[0], order[1], order[2], order[3], depotid_send_to])
    
        
        
        """To determine if all packages have arrived at the final depot a
        calculation is made to see if the time is later that the latest event
        with that depot_id. If later, all packages can be sent"""
        depot_to_client_departure_times = [0 for _ in range(10)]
        for e in event_list5:
            delay = 15
            depotid = e['depot_id']
            hour = int(e['time'][9:11])
            minutes = int(e['time'][12:14])
            seconds = int(e['time'][15:17])
            total_seconds = hour * 3600 + minutes * 60 + seconds + delay
    
            if total_seconds > depot_to_client_departure_times[int(depotid)-201]:
                depot_to_client_departure_times[int(depotid) - 201] = total_seconds
    
        for e in event_list5:
            productid = e['product_id']
            depotid = e['depot_id']
            clientid = e['client_id']
            time = convert(depot_to_client_departure_times[int(depotid)-201])
            orderid = e['order_id']
    
            event_list7.append(
                {"time": time, "status": "Product in Transit to Client", "order_id": orderid,
                 "client_id": clientid,
                 "product_id": productid,
                 "depot_id": depotid})
            time_list7.append([depot_to_client_departure_times[int(depotid)-201]])
        
        
        """All events are saved in csv files for later release into splunk"""
        keys = event_list1[0].keys()
        with open(os.path.join(app_location, 'order_data', 'order_data_for_today1.csv'), 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(event_list1)
        keys = event_list2[0].keys()
        with open(os.path.join(app_location, 'order_data', 'order_data_for_today2.csv'), 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(event_list2)
        keys = event_list3[0].keys()
        with open(os.path.join(app_location, 'order_data', 'order_data_for_today3.csv'), 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(event_list3)
        keys = event_list4[0].keys()
        with open(os.path.join(app_location, 'order_data', 'order_data_for_today4.csv'), 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(event_list4)
        keys = event_list5[0].keys()
        with open(os.path.join(app_location, 'order_data', 'order_data_for_today5.csv'), 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(event_list5)
        keys = event_list6[0].keys()
        with open(os.path.join(app_location, 'order_data', 'order_data_for_today6.csv'), 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(event_list6)
        keys = event_list7[0].keys()
        with open(os.path.join(app_location, 'order_data', 'order_data_for_today7.csv'), 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(event_list7)
    
    
    
        mycsv = csv.writer(open(os.path.join(app_location, 'order_data', 'order_datatime_for_today1.csv'), 'w', newline =''))
        mycsv.writerows(time_list1)
        mycsv = csv.writer(open(os.path.join(app_location, 'order_data', 'order_datatime_for_today2.csv'), 'w', newline =''))
        mycsv.writerows(time_list2)
        mycsv = csv.writer(open(os.path.join(app_location, 'order_data', 'order_datatime_for_today3.csv'), 'w', newline =''))
        mycsv.writerows(time_list3)
        mycsv = csv.writer(open(os.path.join(app_location, 'order_data', 'order_datatime_for_today4.csv'), 'w', newline =''))
        mycsv.writerows(time_list4)
        mycsv = csv.writer(open(os.path.join(app_location, 'order_data', 'order_datatime_for_today5.csv'), 'w', newline =''))
        mycsv.writerows(time_list5)
        mycsv = csv.writer(open(os.path.join(app_location, 'order_data', 'order_datatime_for_today6.csv'), 'w', newline =''))
        mycsv.writerows(time_list6)
        mycsv = csv.writer(open(os.path.join(app_location, 'order_data', 'order_datatime_for_today7.csv'), 'w', newline =''))
        mycsv.writerows(time_list7)
    
        mycsv = csv.writer(
            open(os.path.join(app_location, 'order_data', 'row.csv'), 'w',
                 newline=''))
        mycsv.writerows([[0]])
    
        
        mycsv = csv.writer(
        open(os.path.join(app_location, 'lookups', 'order_counter.csv'), 'w',
             newline=''))
        mycsv.writerows([[order_counter]])

        