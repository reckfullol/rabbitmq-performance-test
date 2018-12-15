#include "amqp_statistical_recv_v2.h"

static int run(
        amqp_connection_state_t conn,
        int message_count)
{
    int recv = 0;
    /* 多预留点空间 */
    uint64_t *send_timestamp = (uint64_t*)malloc(sizeof(uint64_t) * message_count);
    uint64_t *recv_timestamp = (uint64_t*)malloc(sizeof(uint64_t) * message_count);

    memset(send_timestamp, 0x0, sizeof(uint64_t) * message_count);
    memset(recv_timestamp, 0x0, sizeof(uint64_t) * message_count);

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
                PRT("body_target|%zd\n", body_target);
                body_received = 0;

                while (body_received < body_target)
                {
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
                    PRT("body_received|%zd|body_target|%zd|not match\n",
                            body_received,
                            body_target);
                    break;
                }

                uint64_t last_time = 0;
                ReadLong(frame.payload.body_fragment.bytes, &last_time, 0);
                PRT("last_time|%lu\n", last_time);


                // 统计
                {
                    uint64_t last_time = 0;
                    uint64_t flag = 0;
                    ReadLong(frame.payload.body_fragment.bytes, &last_time, 0);
                    ReadLong((char*)frame.payload.body_fragment.bytes + sizeof(uint64_t), &flag, 0);
                    uint64_t now = now_microseconds();

                    if (flag == 1)
                    {
                        send_timestamp[recv] = last_time;
                        recv_timestamp[recv] = now;
                        ++recv;
                    }
                    else
                    {
                        fprintf(stderr, "invalid rsp|now|%lu|last_time|%lu|offset|%lu\n",
                                static_cast<unsigned long>(now), static_cast<unsigned long>(last_time),
                                static_cast<unsigned long>(now - last_time));
                    }
                }

                /* everything was fine, we can quit now because we received the reply */
                break;
            }
        }

        if (recv == message_count)
        {
            break;
        }
    }

    int m = 0;
    for (m = 0; m < message_count; ++m)
    {
        printf("STAT|%d|now|%llu|last_time|%llu|offset|%llu\n",
                m,
                recv_timestamp[m],
                send_timestamp[m],
                recv_timestamp[m] - send_timestamp[m]);
    }

    return 0;
}

int main(int argc, char const *const *argv) {
    if (argc != 10) {
        fprintf(stderr, "Usage: %s hostname port exchange exchange_type queue_name binding_key message_count user_name password\n", argv[0]);
        return 1;
    }

    char const *hostname = argv[1];
    int port = atoi(argv[2]);
    char const *exchange = argv[3];   /* argv[3]; */
    char const *exchange_type = argv[4];
    char const *queue_name = argv[5];
    char const *binding_key = argv[6]; /* argv[4]; */
    int message_count = atoi(argv[7]);
    char const *user = argv[8];
    char const *password = argv[9];


    int status = 0;
    /* 建立接受通道 */
    amqp_connection_state_t conn;
    {
        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        if (!socket) {
            die("creating TCP socket");
        }

        status = amqp_socket_open(socket, hostname, port);
        if (status) {
            die("opening TCP socket");
        }

        die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                    user, password),
                "Logging in");

        amqp_channel_open(conn, 1);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

//        amqp_bytes_t queue_name;
//        {
//            amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
//            die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
//            queue_name = amqp_bytes_malloc_dup(r->queue);
//            if (queue_name.bytes == NULL) {
//                fprintf(stderr, "Out of memory while copying queue name");
//                return 1;
//            }
//        }

        amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue_name), 0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

        amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange), amqp_cstring_bytes(exchange_type), 0, 0, 0, 0, amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");

        amqp_queue_bind(conn, 1, amqp_cstring_bytes(queue_name), amqp_cstring_bytes(exchange),
                amqp_cstring_bytes(binding_key), amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");

        amqp_basic_consume(conn, 1, amqp_cstring_bytes(queue_name), amqp_empty_bytes, 0, 1, 0,
                amqp_empty_table);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
    }

    run(conn, message_count);

    die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
            "Closing channel");
    die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
            "Closing connection");
    die_on_error(amqp_destroy_connection(conn), "Ending connection");

    return 0;
}

