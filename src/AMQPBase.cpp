/*
 *  AMQPBase.cpp
 *  librabbitmq++
 *
 *  Created by Alexandre Kalendarev on 15.04.10.
 *
 */

#include "AMQPcpp.h"

using namespace std;

AMQPBase::~AMQPBase()
{
	//// showFunName(__FUNCTION__);
	this->closeChannel();
}

void AMQPBase::checkReply(amqp_rpc_reply_t *res)
{
	//// showFunName(__FUNCTION__);
	checkClosed(res);
	if (res->reply_type != AMQP_RESPONSE_NORMAL)
		throw AMQPException(res);
}

void AMQPBase::checkClosed(amqp_rpc_reply_t *res)
{
	//// showFunName(__FUNCTION__);
	if (res->reply_type == AMQP_RESPONSE_SERVER_EXCEPTION && res->reply.id == AMQP_CHANNEL_CLOSE_METHOD)
		opened = 0;
}

void AMQPBase::openChannel()
{
	//// showFunName(__FUNCTION__);
	amqp_channel_open(*cnn, channelNum);
	amqp_rpc_reply_t res = amqp_get_rpc_reply(*cnn);
	if (res.reply_type != AMQP_RESPONSE_NORMAL)
		throw AMQPException(&res);
	opened = 1;
}

void AMQPBase::closeChannel()
{
	//// showFunName(__FUNCTION__);
	//// std::cout << " channelNum |  " << channelNum << std::endl;
	//// std::cout << " cnn |  " << cnn << std::endl;
	//// std::cout << " opened |  " << opened << std::endl;
	if (opened && cnn)
		amqp_channel_close(*cnn, channelNum, AMQP_REPLY_SUCCESS);
}

void AMQPBase::reopen()
{
	//// showFunName(__FUNCTION__);
	if (opened)
		return;
	AMQPBase::openChannel();
}

int AMQPBase::getChannelNum()
{
	//// showFunName(__FUNCTION__);
	return channelNum;
}

void AMQPBase::setParam(short param)
{
	//// showFunName(__FUNCTION__);
	this->parms = param;
}

string AMQPBase::getName()
{
	//// showFunName(__FUNCTION__);
	if (!name.size())
		name = "";
	return name;
}

void AMQPBase::setName(string name)
{
	//// showFunName(__FUNCTION__);
	this->name = name;
}

void AMQPBase::showFunName(const char *name)
{
	//// std::cout << " function name |  " << name << std::endl;
}