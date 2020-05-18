import sys
import csv




class Subscriber:
    def __init__(self, ip, tariff):
        self.ip = ip;
        self.tariff = tariff;
        self.path_graph = 'graph.csv';
        self.f_graph = None;
        self.bytes_amount = 0;

    def open_stream(self):
        self.f_graph = open(self.path_graph, 'w');
        self.f_graph.write('date,bytes\n');

    def close_stream(self):
        self.f_graph.close();

    def add(self, date, byte):
        if ( self.f_graph == None ):
            self.open_stream();

        self.f_graph.write( '{},{}\n'.format(date, byte) );

        self.bytes_amount += int(byte);

    def print_tariffing(self):
        KiB = round(self.bytes_amount / 1024, 2);
        price = round(KiB * self.tariff, 2);

        print( 'Subscriber\'s tariffing: {} RUB for {} KiB.'.format(price, KiB) );




def parse(data_path, ip, tariff):

    
    subscriber = Subscriber(ip, tariff);
    subscriber.open_stream();

    
    f_data = open(data_path, 'r');
    data = csv.DictReader(f_data);

    for row in data:
        if ( subscriber.ip == row['Src_IP_Addr:Port'].split(':')[0] or
             subscriber.ip == row['Dst_IP_Addr:Port'].split(':')[0] ):
            subscriber.add(row['Date_first_seen'], row['Bytes']);

    f_data.close();


    subscriber.close_stream();

    subscriber.print_tariffing();




def main():

    if ( len( sys.argv ) > 1 ):
        data_path = sys.argv[1];
        ip = sys.argv[2];
        tariff = sys.argv[3];
    else:
        data_path = 'data.csv';
        ip = '87.245.198.147';
        tariff = 2;
         
    parse(data_path, ip, tariff)


main();
