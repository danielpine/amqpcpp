#include "AMQPcpp.h"
#include <stdlib.h>
#include <stdio.h>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <string.h>
#include <string>

using namespace std;

int i = 0;

string exceptionMsg = "";

int onCancel(AMQPMessage *message)
{
    cout << "cancel tag=" << message->getDeliveryTag() << endl;
    return 0;
}

AMQP amqp("engine:engine@192.168.106.100:5671");

int onMessage(AMQPMessage *message)
{
    clock_t start = clock();
    string strReply = "";
    string priceRes = "";

    try
    {
        uint32_t j = 0;
        i++;
        uint32_t delivery_tag = message->getDeliveryTag();

        string reply_to = message->getHeader("Reply-to");

        string correlation_id = message->getHeader("correlation_id");

        cout << "#" << i << " tag=" << delivery_tag << " content-type:" << message->getHeader("Content-type");

        cout << " encoding:" << message->getHeader("Content-encoding") << " mode=" << message->getHeader("Delivery-mode") << "\n correlation_id=" << correlation_id << endl;

        cout << " reply-to:" << reply_to << endl;
        if (reply_to.empty())
        {
            cout << " No reply to" << endl;
            //Ack
            message->getQueue()->Ack(delivery_tag);
            return 0;
        }
        // Do price
        char *data = message->getMessage(&j);

        string strIn = data;

        bool redelivered = message->getHeader("redelivered") == "1";

        if (redelivered || exceptionMsg != "")
        {
            // reply_to = "error";
        }
        else
        {
            priceRes = strIn;
        }
        // Reply

        AMQPExchange *ex = amqp.createExchange("jrsc.response");
        AMQPQueue *qut = amqp.createQueue(reply_to);
        qut->Declare(reply_to, 1);
        ex->Declare("jrsc.response", "direct");
        ex->setHeader("Delivery-mode", 1);
        ex->setHeader("Content-type", "application/json");
        ex->setHeader("Content-encoding", "UTF-8");
        ex->setHeader("correlation_id", correlation_id);
        ex->Bind(reply_to, correlation_id);
        if (redelivered)
        {
            strReply = "{'code':0,'msg':'Error : SegmentFault'}";
            printf(" Redelivered %d, Job Error for SegmentFault\n", redelivered);
        }
        else if (exceptionMsg != "")
        {
            strReply = exceptionMsg;
        }
        else
        {
            strReply = priceRes;
        }

        ex->Publish(strReply, correlation_id);
        //Ack
        message->getQueue()->Ack(delivery_tag);
        clock_t end = clock();
        cout << " Job Done, Time Used: " << setprecision(5) << ((double)(end - start) / CLOCKS_PER_SEC) << " s" << endl;
        exceptionMsg = "";
        amqp.closeAMQPBase();
        amqp.closeAMQPBase();
        // free(qut);
        // free(ex);
        // qut = 0;
        // ex = 0;
        cout << " qut: " << qut << endl;
        cout << " ex: " << ex << endl;
    }
    catch (AMQPException e)
    {
        std::cout << "========================Exception========================\n"
                  << e.getMessage() << std::endl;
        exceptionMsg = "{'code':0,'msg':'Error : " + e.getMessage() + "'}";
        onMessage(message);
    }

    return 0;
};

int main(int argc, char *argv[])
{

    try
    {
        string consumer_name = "Pricer";
        if (argc > 1)
        {
            consumer_name += "_";
            consumer_name += argv[1];
        }
        cout << "Start Rabbit MQ Consumer " << consumer_name << endl;
        //AMQP amqp("jrsc:jrsc@192.168.106.100:5671");
        cout << "CreateQueue" << endl;
        AMQPQueue *qu2 = amqp.createQueue("ha-request-high-freq");

        cout << "Set Qos 0x1" << endl;
        qu2->Qos(0, 0x1, 0);
        cout << "Declare" << endl;
        qu2->Declare();
        qu2->Bind("amq.direct", "ha-request-high-freq");
        qu2->setConsumerTag(consumer_name);
        qu2->addEvent(AMQP_MESSAGE, onMessage);
        qu2->addEvent(AMQP_CANCEL, onCancel);
        cout << "Rabbit started, waiting messages..." << endl;
        // qu2->Consume(AMQP_NOACK);
        qu2->Consume();
        qu2->closeChannel();
        qu2->Cancel(consumer_name);
        free(qu2);
        qu2 = 0;
        cout << "Rabbit END." << endl;
    }
    catch (AMQPException e)
    {
        std::cout << e.getMessage() << std::endl;
    }
    catch (exception e)
    {
        std::cout << e.what() << std::endl;
    }
    cout << "Main END." << endl;
    return 0;
}
