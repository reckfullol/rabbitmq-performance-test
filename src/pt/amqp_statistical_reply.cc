#include "amqp_statistical_reply.h"

static int run(
        amqp_connection_state_t conn,
        amqp_connection_state_t conn_send,
        char const *exchange,
        char const *routing_key
        )
{
    for (;;)
    {
        /*
           wait an answer
           */
        {
            amqp_frame_t frame;
            int result;

            amqp_basic_deliver_t *d;
            amqp_basic_properties_t *p;
            size_t body_target;
            size_t body_received;

            for (;;)
            {
                amqp_maybe_release_buffers(conn);
                result = amqp_simple_wait_frame(conn, &frame);
                PRT("\n\n==========================\n");
                PRT("Result: %d\n", result);
                if (result < 0) {
                    break;
                }

                PRT("Frame type: %u channel: %u\n", frame.frame_type, frame.channel);
                if (frame.frame_type != AMQP_FRAME_METHOD) {
                    continue;
                }

                PRT("Method: %s\n", amqp_method_name(frame.payload.method.id));
                if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
                    continue;
                }

                d = (amqp_basic_deliver_t *)frame.payload.method.decoded;
                PRT("Delivery: %u exchange: %.*s routingkey: %.*s\n",
                        (unsigned)d->delivery_tag, (int)d->exchange.len,
                        (char *)d->exchange.bytes, (int)d->routing_key.len,
                        (char *)d->routing_key.bytes);

                result = amqp_simple_wait_frame(conn, &frame);
                if (result < 0) {
                    break;
                }

                if (frame.frame_type != AMQP_FRAME_HEADER) {
                    fprintf(stderr, "Expected header!");
                    abort();
                }
                p = (amqp_basic_properties_t *)frame.payload.properties.decoded;
                if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
                    PRT("Content-type: %.*s\n", (int)p->content_type.len,
                            (char *)p->content_type.bytes);
                }
                PRT("----\n");

                body_target = (size_t)frame.payload.properties.body_size;
                body_received = 0;

                while (body_received < body_target) {
                    result = amqp_simple_wait_frame(conn, &frame);
                    if (result < 0) {
                        break;
                    }

                    if (frame.frame_type != AMQP_FRAME_BODY) {
                        fprintf(stderr, "Expected body!");
                        abort();
                    }

                    body_received += frame.payload.body_fragment.len;
                    assert(body_received <= body_target);

                    /*
                       amqp_dump(frame.payload.body_fragment.bytes,
                       frame.payload.body_fragment.len);
                       */
                }

                if (body_received != body_target) {
                    /* Can only happen when amqp_simple_wait_frame returns <= 0 */
                    /* We break here to close the connection */
                    break;
                }

                uint64_t last_time = 0;
                ReadLong(frame.payload.body_fragment.bytes, &last_time, 0);
                PRT("last_time|%lu\n", last_time);

                // send Recv
                {
                    /*
                       set properties
                       */
                    amqp_basic_properties_t props;
                    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
                        AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_REPLY_TO_FLAG |
                        AMQP_BASIC_CORRELATION_ID_FLAG;
                    props.content_type = amqp_cstring_bytes("text/plain");
                    props.delivery_mode = 3;

                    char message[256];
                    memcpy(message, frame.payload.body_fragment.bytes,
                            frame.payload.body_fragment.len);

                    int pos = sizeof(unsigned long);
                    WriteLong(message+pos, 1, 0);

                    amqp_bytes_t message_bytes;
                    message_bytes.len = frame.payload.body_fragment.len;
                    message_bytes.bytes = message;

                    die_on_error(amqp_basic_publish(conn_send, 1,
                                amqp_cstring_bytes(exchange),
                                amqp_cstring_bytes(routing_key),
                                0, 0,
                                // &props,
                                NULL,
                                message_bytes),
                            "Publishing");
                }

                /* everything was fine, we can quit now because we received the reply */
                break;
            }
        }
    }

    return 0;
}

int main(int argc, char const *const *argv) {
    if (argc != 14) {
        fprintf(stderr, "Usage: %s recv_hostname recv_port send_hostname send_port recv_exchange_name recv_exchange_type send_exchange_name send_exchange_type recv_queue binding_key routing_key user_name password\n", argv[0]);
        return 1;
    }

    char const *recv_hostname = argv[1];
    int recv_port = atoi(argv[2]);
    char const *send_hostname = argv[3];
    int send_port = atoi(argv[4]);
    char const *recv_exchange = argv[5];   /* argv[3]; */
    char const *recv_exchange_type = argv[6];
    char const *send_exchange = argv[7];   /* argv[3]; */
    char const *send_exchange_type = argv[8];
    char const *recv_queue = argv[9];
    char const *binding_key = argv[10]; /* argv[4]; */
    char const *routing_key = argv[11];
    char const *user = argv[12];
    char const *password = argv[13];

    int status = 0;
    /* 建立接受通道 */
    amqp_connection_state_t conn_recv;
    {
        conn_recv = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn_recv);
        if (!socket) {
            die("creating TCP socket");
        }

        status = amqp_socket_open(socket, recv_hostname, recv_port);
        if (status) {
            die("opening TCP socket");
        }

        die_on_amqp_error(amqp_login(conn_recv, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                    user, password),
                "Logging in");

        amqp_channel_open(conn_recv, 1);
        die_on_amqp_error(amqp_get_rpc_reply(conn_recv), "Opening channel");

        amqp_exchange_declare(conn_recv, 1, amqp_cstring_bytes(recv_exchange), amqp_cstring_bytes(recv_exchange_type), 0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn_recv), "Declaring exchange");

        amqp_queue_declare(conn_recv, 1, amqp_cstring_bytes(recv_queue), 0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn_recv), "Declaring queue");

        amqp_queue_bind(conn_recv, 1, amqp_cstring_bytes(recv_queue), amqp_cstring_bytes(recv_exchange),
                amqp_cstring_bytes(binding_key), amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn_recv), "Binding queue");

        amqp_basic_consume(conn_recv, 1, amqp_cstring_bytes(recv_queue), amqp_empty_bytes, 0, 1, 0,
                amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn_recv), "Consuming");
    }

    /* 建立发送通道 */
    amqp_connection_state_t conn_send;
    {
        conn_send = amqp_new_connection();

        amqp_socket_t *socket = amqp_tcp_socket_new(conn_send);
        if (!socket) {
            die("creating send TCP socket");
        }

        status = amqp_socket_open(socket, send_hostname, send_port);
        if (status) {
            die("opening TCP socket");
        }

        die_on_amqp_error(amqp_login(conn_send, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                    user, password),
                "Logging in");

        amqp_channel_open(conn_send, 1);
        die_on_amqp_error(amqp_get_rpc_reply(conn_send), "Opening channel");

        amqp_exchange_declare(conn_send, 1, amqp_cstring_bytes(send_exchange), amqp_cstring_bytes(send_exchange_type), 0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn_send), "Declaring exchange");
    }

    run(conn_recv, conn_send, send_exchange, routing_key);

    die_on_amqp_error(amqp_channel_close(conn_recv, 1, AMQP_REPLY_SUCCESS),
            "Closing channel");
    die_on_amqp_error(amqp_connection_close(conn_recv, AMQP_REPLY_SUCCESS),
            "Closing connection");
    die_on_error(amqp_destroy_connection(conn_recv), "Ending connection");

    die_on_amqp_error(amqp_channel_close(conn_send, 1, AMQP_REPLY_SUCCESS),
            "Closing channel");
    die_on_amqp_error(amqp_connection_close(conn_send, AMQP_REPLY_SUCCESS),
            "Closing connection");
    die_on_error(amqp_destroy_connection(conn_send), "Ending connection");

    return 0;
}
