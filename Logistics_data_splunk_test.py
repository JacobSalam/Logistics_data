import pandas as pd
import pickle
import random as r
import csv
import copy
import datetime
import os
from math import sin, cos, sqrt, atan2, radians

r.seed(34)





with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/clients.csv') as csvfile:
    df = csv.reader(csvfile, delimiter=';')
    clients = [list(row) for row in df][1:]
with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/product_lookup.csv') as csvfile:
    df = csv.reader(csvfile, delimiter=';')
    products = [list(row) for row in df][1:]
with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/suppliers.csv') as csvfile:
    df = csv.reader(csvfile, delimiter=';')
    suppliers = [list(row) for row in df][1:]
with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/depots.csv') as csvfile:
    df = csv.reader(csvfile, delimiter=';')
    depots = [list(row) for row in df][1:]
with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/crew.csv') as csvfile:
    df = csv.reader(csvfile, delimiter=';')
    crew = [list(row) for row in df][1:]
with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/recurrent_orders.csv') as csvfile:
    df = csv.reader(csvfile, delimiter=';')
    recurrent_orders = [list(row) for row in df][1:]


if os.path.exists('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_counter.csv'):
    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_counter.csv') as csvfile:
        df = csv.reader(csvfile, delimiter=',')
        order_counter = int([list(row) for row in df][0][0])

else:
    order_counter = 0


# order_counter = 0

"""Get todays and yesterdays date"""
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

orders_log1 = []
event_list = []

date = datetime.datetime.now()
starthourinseconds = 10*60*60




with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/date.csv') as csvfile:
    df = csv.reader(csvfile, delimiter=',')
    dates = [list(row) for row in df]
    today = dates[0][0]

date_csv = datetime.datetime(int(today[6:8])+2000, int(today[:2]), int(today[3:5]))




def now_to_seconds(date):
    todays_time_in_seconds = int(date.hour) * 60 * 60 + int(date.minute) * 60 + int(date.second)
    return todays_time_in_seconds

def convert(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    return date_csv.strftime("%x") + " " + "%d:%02d:%02d" % (hour, minutes, seconds)

# See if clientids_counter already exist, if not make it
if os.path.exists("/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today1.csv"):

    if today <= datetime.date.today().strftime('%m/%d/%y'):
        fieldname_collect = []
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today1.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            all = [list(row) for row in df]
            fieldnames = all[0]
            for x in range(len(all[1:])):
                fieldname_collect.append(fieldnames)
            event_data = all[1:]
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today2.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            # event_data.extend([list(row) for row in df][1:])
            all = [list(row) for row in df]
            fieldnames = all[0]
            for x in range(len(all[1:])):
                fieldname_collect.append(fieldnames)
            event_data.extend(all[1:])
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today3.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            # event_data.extend([list(row) for row in df][1:])
            all = [list(row) for row in df]
            fieldnames = all[0]
            for x in range(len(all[1:])):
                fieldname_collect.append(fieldnames)
            event_data.extend(all[1:])
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today4.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            # event_data.extend([list(row) for row in df][1:])
            all = [list(row) for row in df]
            fieldnames = all[0]
            for x in range(len(all[1:])):
                fieldname_collect.append(fieldnames)
            event_data.extend(all[1:])
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today5.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            # event_data.extend([list(row) for row in df][1:])
            all = [list(row) for row in df]
            fieldnames = all[0]
            for x in range(len(all[1:])):
                fieldname_collect.append(fieldnames)
            event_data.extend(all[1:])
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today6.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            # event_data.extend([list(row) for row in df][1:])
            all = [list(row) for row in df]
            fieldnames = all[0]
            for x in range(len(all[1:])):
                fieldname_collect.append(fieldnames)
            event_data.extend(all[1:])

        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today1.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            time_data = [list(row) for row in df]
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today2.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            time_data.extend([list(row) for row in df])
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today3.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            time_data.extend([list(row) for row in df])
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today4.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            time_data.extend([list(row) for row in df])
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today5.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            time_data.extend([list(row) for row in df])
        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today6.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            time_data.extend([list(row) for row in df])

        with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/row.csv') as csvfile:
            df = csv.reader(csvfile, delimiter=',')
            row = [list(row) for row in df][0][0]

        sorted_time_list_indexes = sorted(range(len(time_data)), key=lambda k: time_data[k])
        sorted_time_list = sorted(time_data, key=lambda x: x[0])

        time_now = now_to_seconds(date)
        row = int(row)

        for x in range(len(sorted_time_list)):
            if row == len(event_data)-1:
                for file in os.listdir('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/'):
                    if file == "date.csv":
                        mycsv = csv.writer(
                            open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/date.csv', 'w',
                                 newline=''))
                        mycsv.writerows([[(date_csv + datetime.timedelta(days=1)).strftime('%m/%d/%y')]])

                        continue
                    os.remove("/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/" + str(file))

                exit()
            if x < row:
                continue

            if int(float(sorted_time_list[x][0])) < time_now or today < datetime.date.today().strftime('%m/%d/%y'):
                log = event_data[sorted_time_list_indexes[x]]
                # print(log)
                names = fieldname_collect[sorted_time_list_indexes[x]]
                # print(names)

                event = {}

                for y in range(len(names)):
                    event[names[y]] = log[y]

                # event = helper.new_event(str(event_list), time=None, host=None, index=None, source=None, sourcetype=None,
                #                          done=True, unbroken=True)
                # ew.write_event(event)
                print(event)


            else:
                break
        mycsv = csv.writer(
            open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/row.csv', 'w',
                 newline=''))
        mycsv.writerows([[x]])



elif today <= datetime.date.today().strftime('%m/%d/%y'):
    # Get a random number of new orders
    opt_number_of_orders_per_day = 750
    opt_std_dev_number_of_orders = 250
    num_new_orders = r.randrange(500, 1000)


    # create a random probablity for the orders
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

    time = starthourinseconds
    day_of_the_week = date_csv.strftime('%A')

    for x in recurrent_orders:
        if x[0] == day_of_the_week:
            clientid = int(x[1]) - 10900
            product_id = int(x[2]) - 30000
            orders_log1.append([order_counter, clientid, product_id, products[product_id - 1][6], time])
            event_list1.append(
                {"time": convert(time), "status": "Order Received from Client", "order_id": order_counter,
                 "client_id": clientid + 10900, "product_id": product_id + 30000,
                 "supplier_id": products[product_id - 1][6]})
            time_list1.append([time])
            order_counter += 1

    for n in range(num_new_orders):
        product_id = product_probs[r.sample(range(10000), 1)[0]]
        clientid = r.randrange(1, 281)
        orders_log1.append([order_counter, clientid, product_id, products[product_id - 1][6], time])
        event_list1.append(
            {"time": convert(time), "status": "Order Received from Client", "order_id": order_counter,
             "client_id": clientid + 10900, "product_id": product_id + 30000,
             "supplier_id": products[product_id - 1][6]})
        time_list1.append([time])

        order_counter += 1
    total_orders = len(orders_log1)


    '''
    # 0: order_id, 1: client_id, 2: product_id, 3: supplier_id
    
    # create distance csv for suppliers to depots
    depot_collect = []
    R = 6373.0
    for s in suppliers:
        # print(order[3])
        print(s)
        lats = radians(float(s[3]))
        lons = radians(float(s[4]))
        dist = 100000000
        best = 0
        for d in depots:
            print(d)
            latd = radians(float(d[3]))
            lond = radians(float(d[4]))
    
            dlon = lons - lond
            dlat = lats - latd
    
            a = sin(dlat / 2) ** 2 + cos(lats) * cos(latd) * sin(dlon / 2) ** 2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
            distance = R * c
    
            # distance = ((lats - latd) ** 2 + (lons - lond) ** 2) ** 0.5
            if distance < dist:
                dist = distance
                best = d[0]
        depot_collect.append([best, dist])
    
    
    
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/nearest_depots_from_suppliers.csv', 'a+', newline =''))
    # for row in depot_collect:
    mycsv.writerows(depot_collect)
    '''

    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/nearest_depots_from_suppliers.csv') as csvfile:
        df = csv.reader(csvfile, delimiter=',')
        nearest_depots = [list(row) for row in df]

    # print(nearest_depots)
    '''
    # create distance csv for suppliers to depots
    R = 6373.0
    depot_to_depot_distance = []
    for d1 in depots:
        lats = radians(float(d1[3]))
        lons = radians(float(d1[4]))
        dist = []
        for d2 in depots:
            # print(d)
            latd = radians(float(d2[3]))
            lond = radians(float(d2[4]))
    
            dlon = lons - lond
            dlat = lats - latd
    
            a = sin(dlat / 2) ** 2 + cos(lats) * cos(latd) * sin(dlon / 2) ** 2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
            distance = R * c
    
            # distance = ((lats - latd) ** 2 + (lons - lond) ** 2) ** 0.5
            dist.append(distance)
    
        depot_to_depot_distance.append(dist)
    
    
    
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/depot_to_depot_distance.csv', 'a+', newline =''))
    # for row in depot_collect:
    mycsv.writerows(depot_to_depot_distance)
    '''

    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/depot_to_depot_distance.csv') as csvfile:
        df = csv.reader(csvfile, delimiter=',')
        depot_to_depot_distance = [list(row) for row in df]


    orders_log2 = []

    # for d in depots:
    #     sec_to_bus_for_transit.append(r.randrange(600, 1800))

    for order in orders_log1:
        supplierid = order[3]
        depotid = nearest_depots[int(supplierid)-101][0]

        time = starthourinseconds+r.randrange(600, 1800)

        event_list2.append(
            {"time": convert(time), "status": "Product in transit to depot", "order_id": order[0], "client_id": order[1] + 10900, "product_id": order[2]+ 30000,
             "supplier_id": supplierid, "depot_id": depotid})
        time_list2.append([time])


        # print({"status": "Product in transit to depot", "order_id": order[0], "client_id": order[1], "product_id": order[2],
        #      "supplier_id": supplierid, "depot_id": depotid})

        # Add distance/expected time calculation to simulate delivery time
        depotid = nearest_depots[int(supplierid) - 101][0]
        distance = nearest_depots[int(supplierid) - 101][1]
        time = time + float(distance)/60*3600

        event_list3.append(
            {"time": convert(time), "status": "Product arrived at Nearest depot", "order_id": order[0], "client_id": order[1]+ 10900, "product_id": order[2]+ 30000,
             "supplier_id": supplierid, "depot_id": depotid})
        time_list3.append([time])

        orders_log2.append([order[0], order[1], order[2], order[3], depotid, time])

    '''
    R = 6373.0
    depot_to_client_distance = []
    for c in clients:
        lats = radians(float(c[3]))
        lons = radians(float(c[4]))
        dist = []
        for d in depots:
            latd = radians(float(d[3]))
            lond = radians(float(d[4]))
            # distance = ((lats - latd) ** 2 + (lons - lond) ** 2) ** 0.5
            dlon = lons - lond
            dlat = lats - latd
    
            a = sin(dlat / 2) ** 2 + cos(lats) * cos(latd) * sin(dlon / 2) ** 2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            distance = R * c
            dist.append(distance)
    
        depot_to_client_distance.append(dist)
    
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/nearest_depots_to_clients.csv', 'a+', newline =''))
    # for row in depot_collect:
    mycsv.writerows(depot_to_client_distance)
    
    '''


    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/nearest_depots_to_clients.csv') as csvfile:
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

        if depotid_send_to == depotid_current:
            depotid_to = int(depotid_send_to) - 201
            crew_size = len(crew_list_by_depot[depotid_to])
            crew_choice = r.randrange(0,crew_size)
            crew_id = crew_list_by_depot[depotid_to][crew_choice][0]
            crew_seniority = crew_list_by_depot[depotid_to][crew_choice][1]

            event_list4.append(
                {"time": convert(time), "status": "Product arrived at final depot", "order_id": order[0], "client_id": order[1]+10900, "product_id": order[2]+30000,
                 "current_depot_id": depotid_current, "previous_depot_id": depotid_send_to, "crew_id": crew_id})
            time_list4.append([time])

            if r.randrange(11) > int(crew_seniority):
                continue
            # time = order[5]
            sec_to_bus1_to_bus2 = r.randrange(600, 1800)
            time = time + sec_to_bus1_to_bus2

            event_list5.append(
                {"time": convert(time), "status": "Product Ready for client delivery", "order_id": order[0], "client_id": order[1] + 10900,
                 "product_id": order[2] + 30000,
                 "depot_id": depotid_current})
            time_list5.append([time])

            ordercheck1[depotid_to] += 1

            orders_log3.append([order[0], order[1], order[2], order[3], order[4]])

        else:
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
                 "current_depot_id": depotid_current, "crew_id": crew_id_from})
            time_list6.append([time])

            if r.randrange(11) > int(crew_seniority_from):
                continue

            distance = depot_to_depot_distance[depotid_from][depotid_to]
            time = time + float(distance)/60*3600


            event_list4.append(
                {"time": convert(time), "status": "Product arrived at final depot", "order_id": order[0], "client_id": order[1]+10900,
                 "product_id": order[2]+30000,
                 "current_depot_id": depotid_send_to, "previous_depot_id": depotid_current, "crew_id": crew_id_to})
            time_list4.append([time])

            if r.randrange(11) > int(crew_seniority_to):
                continue

            sec_to_bus1_to_bus2 = r.randrange(600, 1800)
            time = time + sec_to_bus1_to_bus2

            # print({"time": convert(time), "status": "Product Ready for client delivery", "order_id": order[0], "client_id": order[1] + 10900,
            #      "product_id": order[2] + 30000,
            #      "depot_id": depotid_send_to})
            event_list5.append(
                {"time": convert(time), "status": "Product Ready for client delivery", "order_id": order[0], "client_id": order[1] + 10900,
                 "product_id": order[2] + 30000,
                 "depot_id": depotid_send_to})
            time_list5.append([time])

            ordercheck1[depotid_to] += 1

            orders_log3.append([order[0], order[1], order[2], order[3], depotid_send_to])

    depot_to_client_departure_times = [0 for _ in range(10)]
    for e in event_list5:

        depotid = e['depot_id']
        hour = int(e['time'][9:11])
        minutes = int(e['time'][12:14])
        seconds = int(e['time'][15:17])
        total_seconds = hour * 3600 + minutes * 60 + seconds

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

    # print(event_list7)



    keys = event_list1[0].keys()
    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today1.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(event_list1)

    keys = event_list2[0].keys()
    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today2.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(event_list2)

    keys = event_list3[0].keys()
    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today3.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(event_list3)

    keys = event_list4[0].keys()
    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today4.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(event_list4)

    keys = event_list5[0].keys()
    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today5.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(event_list5)
    keys = event_list6[0].keys()
    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today6.csv', 'w', newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(event_list6)
    keys = event_list7[0].keys()
    with open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_data_for_today7.csv', 'w',
              newline='')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(event_list7)



    # print(time_list1)
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today1.csv', 'w', newline=''))
    mycsv.writerows(time_list1)
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today2.csv', 'w', newline=''))
    mycsv.writerows(time_list2)
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today3.csv', 'w', newline=''))
    mycsv.writerows(time_list3)
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today4.csv', 'w', newline=''))
    mycsv.writerows(time_list4)
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today5.csv', 'w', newline=''))
    mycsv.writerows(time_list5)
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today6.csv', 'w', newline=''))
    mycsv.writerows(time_list6)
    mycsv = csv.writer(open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/order_datatime_for_today7.csv', 'w', newline=''))
    mycsv.writerows(time_list7)

    mycsv = csv.writer(
        open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/row.csv', 'w',
             newline=''))
    mycsv.writerows([[0]])

    # today = datetime.date.today().strftime('%m/%d/%y')
    # mycsv = csv.writer(
    #     open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_data/date.csv', 'w',
    #          newline=''))
    # mycsv.writerows([[today]])

    mycsv = csv.writer(
        open('/Users/jacobsalam/Logistics Show Case/splunk_log_data/order_counter.csv', 'w',
             newline=''))
    mycsv.writerows([[order_counter]])


    # find last item to be to ready for delivery for each depot, depending on this create events for log






    # for x in range(len(time_list)):
    #     print(time_list[x], sorted_time_list[x])
    # print(sorted_time_list)


# make new count for each Product Ready for client status per depot_id --> can be used to determine when it is time to send out the packages by bus