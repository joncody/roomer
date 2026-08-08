import roomer from "/src/roomer.js";

const decoder = new TextDecoder("utf-8");
const root = roomer("ws://localhost:8080/ws");

root.on("open", function () {
    console.log("Joined root room. Client ID: " + root.id());

    const lobby = root.join("lobby");

    lobby.on("open", function () {
        console.log("Joined lobby channel!");
        lobby.send("chat", "Hello room!");
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
