import roomer from "/client/roomer.js";

const decoder = new TextDecoder("utf-8");

// Automatically use the host/port and protocol of the current page
const protocol = location.protocol === "https:" ? "wss:" : "ws:";
const ws_url = protocol + "//" + location.host + "/ws";

const root = roomer(ws_url, { reconnect: true });

root.on("open", function () {
    console.log("Joined root room. Client ID: " + root.id());

    const lobby = root.join("lobby");

    lobby.on("open", function () {
        console.log("Joined lobby channel!");
        lobby.send("chat", "Hello from " + location.host + "!");
    });

    lobby.on("chat", function (payload, sender_id) {
        const text = decoder.decode(payload);
        console.log("[" + sender_id + "]: " + text);
    });

    lobby.on("new_member", function (id) {
        console.log("User joined: " + id);
    });

    lobby.on("member_left", function (id) {
        console.log("User left: " + id);
    });
});
