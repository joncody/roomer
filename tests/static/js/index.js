import bytecursor from "/client/bytecursor.js";
import roomer from "/client/roomer.js";

function create_test_runner() {
    const results_container = document.getElementById("test-results");
    const summary_container = document.getElementById("summary");

    let current_group_body = null;
    let failed_assertions = 0;
    let passed_assertions = 0;
    let total_assertions = 0;

    function group(title) {
        if (current_group_body !== null) {
            console.groupEnd();
        }
        console.group(title);

        const group_el = document.createElement("div");
        const header_el = document.createElement("div");

        group_el.className = "test-group";
        header_el.className = "group-header";
        header_el.textContent = title;

        current_group_body = document.createElement("div");
        current_group_body.className = "group-body";

        group_el.appendChild(header_el);
        group_el.appendChild(current_group_body);
        results_container.appendChild(group_el);
    }

    function assert(condition, message) {
        total_assertions += 1;
        const entry = document.createElement("div");

        if (condition === true) {
            passed_assertions += 1;
            entry.className = "log-entry pass";
            entry.textContent = "[PASS] " + message;
            console.log("[PASS] " + message);
        } else {
            failed_assertions += 1;
            entry.className = "log-entry fail";
            entry.textContent = "[FAIL] " + message;
            console.error("[FAIL] " + message);
        }

        if (current_group_body !== null) {
            current_group_body.appendChild(entry);
        }
    }

    function assert_throws(fn, message) {
        total_assertions += 1;
        const entry = document.createElement("div");
        try {
            fn();
            failed_assertions += 1;
            entry.className = "log-entry fail";
            entry.textContent = "[FAIL] " + message + " (Did not throw)";
            console.error("[FAIL] " + message + " (Did not throw)");
        } catch (ignore) {
            passed_assertions += 1;
            entry.className = "log-entry pass";
            entry.textContent = "[PASS] " + message + " (Threw as expected)";
            console.log("[PASS] " + message + " (Threw as expected)");
        }

        if (current_group_body !== null) {
            current_group_body.appendChild(entry);
        }
    }

    function render_summary(start_time) {
        if (current_group_body !== null) {
            console.groupEnd();
        }

        const elapsed = performance.now() - start_time;
        const duration = Math.round(elapsed * 100) / 100;
        let status_class = "summary-fail";

        if (failed_assertions === 0) {
            status_class = "summary-pass";
        }

        const summary_text = (
            "Total Assertions: " +
            total_assertions +
            " | Passed: " +
            passed_assertions +
            " | Failed: " +
            failed_assertions +
            " | Execution Time: " +
            duration +
            " ms"
        );

        console.info(summary_text);

        summary_container.innerHTML = (
            "Total Assertions: <strong>" +
            total_assertions +
            "</strong> | Passed: <span class='" +
            status_class +
            "'>" +
            passed_assertions +
            "</span> | Failed: <span class='" +
            status_class +
            "'>" +
            failed_assertions +
            "</span> | Execution Time: <strong>" +
            duration +
            " ms</strong>"
        );
    }

    return Object.freeze({
        assert,
        assert_throws,
        group,
        render_summary
    });
}

function encode_packet(room, event, dst, src, payload_str) {
    const encoder = new TextEncoder();
    const room_bytes = encoder.encode(room);
    const event_bytes = encoder.encode(event);
    const dst_bytes = encoder.encode(dst);
    const src_bytes = encoder.encode(src);
    const payload_bytes = encoder.encode(payload_str);
    const total_bytes = (
        room_bytes.byteLength +
        event_bytes.byteLength +
        dst_bytes.byteLength +
        src_bytes.byteLength +
        payload_bytes.byteLength +
        20
    );

    const data = bytecursor(new ArrayBuffer(total_bytes));
    data.writeUint32(room_bytes.byteLength);
    data.writeBytes(room_bytes);
    data.writeUint32(event_bytes.byteLength);
    data.writeBytes(event_bytes);
    data.writeUint32(dst_bytes.byteLength);
    data.writeBytes(dst_bytes);
    data.writeUint32(src_bytes.byteLength);
    data.writeBytes(src_bytes);
    data.writeUint32(payload_bytes.byteLength);
    data.writeBytes(payload_bytes);

    data.rewind();
    return data.getBytes().buffer;
}

function parse_packet_data(buffer) {
    const data = bytecursor(buffer);
    const room_str = data.getString(data.getUint32());
    const event_str = data.getString(data.getUint32());
    const dst_str = data.getString(data.getUint32());
    const src_str = data.getString(data.getUint32());
    const payload_bytes = data.getBytes(data.getUint32());

    return {
        dst: dst_str,
        event: event_str,
        payload: payload_bytes,
        room: room_str,
        src: src_str
    };
}

function run_all_tests() {
    const runner = create_test_runner();
    const start_time = performance.now();

    // -------------------------------------------------------------------------
    // GROUP 1: Roomer Instantiation & Argument Validation
    // -------------------------------------------------------------------------
    runner.group("1. Roomer Instantiation & Argument Validation");

    runner.assert_throws(function () {
        roomer(12345);
    }, "roomer() with non-string URL throws TypeError");

    const ws_url = "ws://" + location.host + "/ws";
    const root = roomer(ws_url);
    runner.assert(
        root.name === "root",
        "roomer() returns root room instance"
    );
    runner.assert(
        typeof root.join === "function",
        "Root room instance includes join() method"
    );
    runner.assert(
        typeof root.purge === "function",
        "Root room instance includes purge() method"
    );

    // -------------------------------------------------------------------------
    // GROUP 2: Member ID & Initial State
    // -------------------------------------------------------------------------
    runner.group("2. Member ID & Initial State");

    runner.assert(
        root.open() === false,
        "Initial open() state is false before join_ack"
    );
    runner.assert(
        root.id() === "",
        "Initial id() is empty string before join_ack"
    );
    runner.assert(
        Array.isArray(root.members()) === true && root.members().length === 0,
        "Initial members() returns empty array copy"
    );

    // -------------------------------------------------------------------------
    // GROUP 3: Packet Parsing & State Transitions
    // -------------------------------------------------------------------------
    runner.group("3. Packet Parsing & State Transitions");

    let open_fired = false;
    root.on("open", function () {
        open_fired = true;
    });

    const join_ack_buffer = encode_packet(
        "root",
        "join_ack",
        "",
        "user_123",
        JSON.stringify(["user_123", "user_456"])
    );

    const packet = parse_packet_data(join_ack_buffer);
    root.parse(packet);

    runner.assert(
        open_fired === true,
        "join_ack packet triggers 'open' event on room"
    );
    runner.assert(
        root.open() === true,
        "open() returns true after join_ack"
    );
    runner.assert(
        root.id() === "user_123",
        "id() matches assigned member ID from join_ack"
    );
    runner.assert(
        root.members().length === 2 && root.members()[0] === "user_123",
        "members() contains parsed member list"
    );

    // -------------------------------------------------------------------------
    // GROUP 4: Member Joining & Leaving Events
    // -------------------------------------------------------------------------
    runner.group("4. Member Joining & Leaving Events");

    let new_member_id = null;
    root.on("new_member", function (id) {
        new_member_id = id;
    });

    const new_member_buffer = encode_packet(
        "root",
        "new_member",
        "user_123",
        "user_789",
        "user_789"
    );
    const packet2 = parse_packet_data(new_member_buffer);

    root.parse(packet2);

    runner.assert(
        new_member_id === "user_789",
        "new_member packet triggers 'new_member' event with payload"
    );
    runner.assert(
        root.members().length === 3,
        "members() updated to include new member (3)"
    );

    let left_member_id = null;
    root.on("member_left", function (id) {
        left_member_id = id;
    });

    const left_buffer = encode_packet(
        "root",
        "member_left",
        "user_123",
        "user_789",
        "user_789"
    );
    const packet3 = parse_packet_data(left_buffer);

    root.parse(packet3);

    runner.assert(
        left_member_id === "user_789",
        "member_left packet triggers 'member_left' event"
    );
    runner.assert(
        root.members().length === 2,
        "members() updated to remove left member (2)"
    );

    // -------------------------------------------------------------------------
    // GROUP 5: Custom Event Delivery & Reserved Guards
    // -------------------------------------------------------------------------
    runner.group("5. Custom Event Delivery & Reserved Guards");

    let custom_payload = null;
    root.on("chat_msg", function (payload) {
        custom_payload = new TextDecoder().decode(payload);
    });

    const msg_buffer = encode_packet(
        "root",
        "chat_msg",
        "user_123",
        "user_456",
        "Hello World!"
    );
    const packet4 = parse_packet_data(msg_buffer);

    root.parse(packet4);

    runner.assert(
        custom_payload === "Hello World!",
        "Custom event packet delivers payload to event listener"
    );

    runner.assert_throws(function () {
        root.send("join", "invalid");
    }, "send() with reserved event name ('join') throws Error");

    // -------------------------------------------------------------------------
    // GROUP 6: Listener Clearing & Exception Guards
    // -------------------------------------------------------------------------
    runner.group("6. Listener Clearing & Exception Guards");

    let count_a = 0;
    let count_b = 0;

    root.on("evt_a", function () {
        count_a += 1;
    });
    root.on("evt_b", function () {
        count_b += 1;
    });

    root.clearListeners(["evt_a"]);
    root.emit("evt_a");
    root.emit("evt_b");

    runner.assert(
        count_a === 1,
        "clearListeners(['evt_a']) preserves excepted event listener"
    );
    runner.assert(
        count_b === 0,
        "clearListeners(['evt_a']) clears non-excepted event listener"
    );

    // -------------------------------------------------------------------------
    // GROUP 7: Force Close & Purge
    // -------------------------------------------------------------------------
    runner.group("7. Force Close & Purge");

    let close_fired = false;
    root.on("close", function () {
        close_fired = true;
    });

    root.forceClose();

    runner.assert(
        close_fired === true,
        "forceClose() triggers 'close' event"
    );
    runner.assert(
        root.open() === false,
        "open() returns false after forceClose()"
    );

    runner.render_summary(start_time);
}

run_all_tests();
