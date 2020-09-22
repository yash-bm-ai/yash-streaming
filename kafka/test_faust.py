from asyncio import sleep

import faust

# ssl_context = ssl.create_default_context(
#     purpose=ssl.Purpose.SERVER_AUTH, cafile='/tmp/newssl/ca.cer')
#
# ssl_context.load_cert_chain('/tmp/newssl/client.cer', keyfile='/tmp/newssl/client.key')

app = faust.App('group1',
                broker=['kafka://localhost:9094', 'kafka://localhost:9093', 'kafka://localhost:9092'],
                value_serialize="raw"
                # enable_auto_commit=False   ## Doesn't work
                # broker_credentials=ssl_context
                )

topic = app.topic('my-kafka-topic')

'''
For batch stream.take(10, within=60), It will take 10 msg at once.
If within 60sec, all 10 are not produced, then the existing msg will come.
print(len(event))
'''

'''
msg 1(P2) > 2 > 3 > 4 > 5(P1)

In case of concurrency, different agents will work concurrently.
In case of msg 5 (Partition: 1) is finished first and ack'ed, and msg 1 (partition 2) is taking time:
    - partition 1 (msg 5) will be comitted
    - partition 2 (msg 1) will be comitted only when 1 is ack'ed. 
        - Given msg 1 is exception, in manual mode, all msg in that partition after 1 will be read again. 
          as none were comitted after 1
'''


@app.agent(topic)
async def process(stream):
    # async for event in stream.noack().events():
    #     print(event.value)
    #
    #     if (event.value["-"] % 12 == 0 and event.value["-"]!=0):
    #         print("Exception------------------ for "+str(event.value["-"]))
    #         #event.ack()
    #     else:
    #         done = event.ack()   ## Commenting this will not ack the msg
    #         print("Ack done: "+str(done)+" "+str(event.value["-"]))

    # async for event in stream:  # .noack().events()
    #     # print(event["-"])
    #     print(event)
    #
    #     # if(event["-"] % 100==0):
    #     #     raise Exception("lolol")
    #     #sleep(0.05)
    #     sleep(10)
    #
    #     print("sleep done")
    #     ## Ack here, at the end

    async for event in stream:

        try:
            print(event.value)

            data = event.value["-"]
            if (data == 1):
                await sleep(15)
                # raise Exception("1111- exception")
                print("sleep done for 1")
            elif (data == 2):
                await sleep(4)
                print("sleep done for 2")
            elif (data == 3):
                await sleep(3)
                print("sleep done for 3")
            elif (data == 4):
                await sleep(2)
                print("sleep done for 4")
            elif (data == 5):
                await sleep(1)
                print("sleep done for 5")

            # done = event.ack()   ## Commenting this will not ack the msg
            # print("Ack done: "+str(done)+" "+str(event.value["-"]))
        except Exception as e:
            print("Main Exception :: " + str(e))
