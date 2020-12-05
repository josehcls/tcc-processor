import paho.mqtt.client as mqtt
import json
import psycopg2

topic = 'topico_teste'
conn_string = "host='tcc.ckbc5tcnnws2.sa-east-1.rds.amazonaws.com' port='5432' dbname ='tcc' user='postgres' password='C51CD2FBB6C6E108439EE6F9015BB95395F8DC89D6546960BBD59D4C71FEA523'"
conn = psycopg2.connect(conn_string)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe(topic)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    try:
        unpacked_json = json.loads(str(msg.payload.decode('utf-8', 'ignore')))
        save_data(unpacked_json)
    except Exception as e:
        print("Couldn't parse raw data: %s" % msg.payload, e)

def save_data(data):
    print('Saving Data: {}'.format(data))
    cursor = conn.cursor()
    variables = data.keys()
    for variable in variables:
        value = data[variable]
        print('variable {}: {}'.format(variable, value))
        sql = ''' INSERT INTO recipe.batch_datapoints 
                    (batch_id, variable, value, moment) VALUES ({0}, '{1}', {2}, now()) 
            '''.format(1, variable, value)
        cursor.execute(sql)
        conn.commit()
    print('Done!')

def main():
    client = mqtt.Client(client_id="pythonzera3")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host='ec2-18-229-135-70.sa-east-1.compute.amazonaws.com', port=1883, keepalive=60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
        print('MQTT client disconnected, exiting now.')

if __name__ == '__main__':
    main()
