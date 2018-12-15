#include "amqp_statistical_send_v2.h"

static int do_batch(
        amqp_connection_state_t conn,
        char const *exchange,
        char const *routing_key,
        int rate_limit, int message_count)
{
    uint64_t start_time = now_microseconds();
    int sent = 0;
    int previous_sent = 0;
    uint64_t previous_report_time = start_time;
    uint64_t next_summary_time = start_time + SUMMARY_EVERY_US;
    // uint64_t next_summary_time = start_time + rate_limit;

    int pos = 0;
    char message[256];
    memset(message, 0x0, sizeof(message));
    amqp_bytes_t message_bytes;

    int i;
    for (i = 0; i < message_count; i++)
    {
        uint64_t now = now_microseconds();
        // 发送消息
        {
            // printf("now|%lu\n", now);
            pos = WriteLong(message, now, 0);
            pos += WriteLong(message+pos, 0, 0);

            message_bytes.len = pos;
            message_bytes.bytes = message;

            die_on_error(amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange),
                        amqp_cstring_bytes(routing_key), 0, 0, NULL,
                        message_bytes),
                    "Publishing");
            sent++;
        }

        now = now_microseconds();
        if (now > next_summary_time)
        {
            int countOverInterval = sent - previous_sent;
            double intervalRate =
                countOverInterval / ((now - previous_report_time) / 1000000.0);
            printf("ms|%llu|%u:\n", now, (int)(now - start_time) / 1000);
            printf("Sent %d - %d since last report (%d Hz)\n",
                    sent, countOverInterval,
                    (int)intervalRate);

            previous_sent = sent;
            previous_report_time = now;
            next_summary_time += SUMMARY_EVERY_US;
            // next_summary_time += rate_limit;
        }

        now = now_microseconds();
        while (((i * 1000000.0) / (now - start_time)) > rate_limit)
        {
            microsleep(1000);
            now = now_microseconds();
        }
    }

    {
        uint64_t stop_time = now_microseconds();
        int total_delta = (int)(stop_time - start_time);

        printf("PRODUCER - Message count: %d\n", message_count);
        printf("Total time, milliseconds: %d\n", total_delta / 1000);
        printf("Overall messages-per-second: %g\n",
                (message_count / (total_delta / 1000000.0)));
    }

    return 0;
}

int main(int argc, char const *const *argv)
{
    if (argc != 10) {
        fprintf(stderr,
                "Usage: %s hostname port exchange exchange_type routing_key rate_limit message_count user_name password\n", argv[0]);
        return 1;
    }

    char const *hostname = argv[1];
    int port = atoi(argv[2]);
    char const *exchange = argv[3];   /* argv[3]; */
    char const *exchange_type = argv[4];
    char const *routing_key = argv[5];
    int rate_limit = atoi(argv[6]);
    int message_count = atoi(argv[7]);
    char const *user = argv[8];
    char const *password = argv[9];
    int status = 0;


    /* 初始化发送queue */
    amqp_connection_state_t conn;
    {
        conn = amqp_new_connection();

        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        if (!socket) {
            die("creating send TCP socket");
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
    }


    amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange), amqp_cstring_bytes(exchange_type), 0, 0, 0, 0, amqp_empty_table);

    do_batch(conn,
            exchange,
            routing_key,
            rate_limit, message_count);

    die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
            "Closing channel");
    die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
            "Closing connection");
    die_on_error(amqp_destroy_connection(conn), "Ending connection");

    return 0;
}

