package co.vinni.dmos.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class NetworkSocketHandler extends TextWebSocketHandler {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm");
    private static final Random RANDOM = new Random();

    private static final Set<WebSocketSession> sessions = new CopyOnWriteArraySet<>();
    private static final Map<String, UserProfile> usersBySession = new ConcurrentHashMap<>();
    private static final Map<String, ChatRoom> roomsById = new ConcurrentHashMap<>();

    static {
        resetToDefaultRooms();
    }

    private static void resetToDefaultRooms() {
        roomsById.clear();
        createDefaultRoom("general", "General", "Conversacion abierta entre todos los usuarios conectados", "chat", "public", "", true);
        createDefaultRoom("deportes", "Deportes", "Partidos, torneos, entrenamientos, resultados y debates", "trophy", "public", "", true);
        createDefaultRoom("musica", "Musica", "Artistas, ritmos, lanzamientos, bandas y recomendaciones", "music", "public", "", true);
    }

    private static void createDefaultRoom(String id, String name, String topic, String iconKey, String privacy, String pin, boolean systemRoom) {
        ChatRoom room = new ChatRoom();
        room.id = id;
        room.name = name;
        room.topic = topic;
        room.iconKey = iconKey;
        room.privacy = privacy;
        room.code = id.toUpperCase(Locale.ROOT);
        room.pin = pin;
        room.creatorSessionId = "SYSTEM";
        room.creatorName = "Sistema";
        room.creatorNationality = "";
        room.creatorAccessCode = "";
        room.systemRoom = systemRoom;
        roomsById.put(id, room);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.add(session);
        sendStateToSession(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            JsonNode root = mapper.readTree(message.getPayload());
            String type = root.path("type").asText("");

            switch (type) {
                case "REGISTER":
                    handleRegister(session, root);
                    break;
                case "LOGOUT":
                    handleLogout(session);
                    break;
                case "JOIN_ROOM":
                    handleJoinRoom(session, root);
                    break;
                case "JOIN_BY_CODE":
                    handleJoinByCode(session, root);
                    break;
                case "CREATE_ROOM":
                    handleCreateRoom(session, root);
                    break;
                case "EDIT_ROOM":
                    handleEditRoom(session, root);
                    break;
                case "DELETE_ROOM":
                    handleDeleteRoom(session, root);
                    break;
                case "CHAT_MESSAGE":
                    handleChatMessage(session, root);
                    break;
                case "KICK_USER":
                    handleKickUser(session, root);
                    break;
                case "BAN_USER":
                    handleBanUser(session, root);
                    break;
                default:
                    sendError(session, "Tipo de evento no reconocido: " + type);
                    break;
            }
        } catch (Exception ex) {
            sendError(session, "Formato de mensaje invalido en el servidor.");
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session);
        removeUserSession(session.getId());
        broadcastAllStates();
    }

    private void removeUserSession(String sessionId) {
        for (ChatRoom r : roomsById.values()) {
            r.authorizedSessions.remove(sessionId);
        }
        UserProfile leaving = usersBySession.remove(sessionId);
        if (leaving != null) {
            ObjectNode sysMsg = mapper.createObjectNode();
            sysMsg.put("type", "SYSTEM_EVENT");
            sysMsg.put("roomId", leaving.roomId);
            sysMsg.put("text", leaving.name + " (" + leaving.nationality + ") se ha desconectado.");
            broadcastToRoom(leaving.roomId, sysMsg);
        }
    }

    private void handleLogout(WebSocketSession session) {
        removeUserSession(session.getId());
        broadcastAllStates();
    }

    private boolean isRoomCreator(UserProfile user, ChatRoom room) {
        if (user == null || room == null || room.systemRoom) {
            return false;
        }
        return room.creatorAccessCode != null
                && !room.creatorAccessCode.isEmpty()
                && user.name.equalsIgnoreCase(room.creatorName)
                && user.nationality.equalsIgnoreCase(room.creatorNationality)
                && user.accessCode.equals(room.creatorAccessCode);
    }

    private void handleRegister(WebSocketSession session, JsonNode root) {
        String rawName = root.path("name").asText("").trim();
        String nationality = root.path("nationality").asText("Colombia").trim();
        String accessCode = root.path("accessCode").asText("").trim();
        String requestedRoom = root.path("roomId").asText("general").trim().toLowerCase(Locale.ROOT);

        if (!rawName.matches("[A-Za-zÁÉÍÓÚáéíóúÑñ ]{2,30}")) {
            sendError(session, "El nombre debe tener entre 2 y 30 letras (sin numeros ni simbolos raros).");
            return;
        }

        if (accessCode.length() < 2 || accessCode.length() > 24) {
            sendError(session, "Ingresa un codigo de acceso (entre 2 y 24 caracteres).");
            return;
        }

        for (ChatRoom r : roomsById.values()) {
            r.authorizedSessions.remove(session.getId());
        }

        UserProfile profile = new UserProfile();
        profile.sessionId = session.getId();
        profile.name = rawName;
        profile.nationality = nationality.isEmpty() ? "Colombia" : nationality;
        profile.accessCode = accessCode;

        for (ChatRoom r : roomsById.values()) {
            if (isRoomCreator(profile, r)) {
                r.creatorSessionId = session.getId();
                r.authorizedSessions.add(session.getId());
            }
        }

        ChatRoom targetRoom = roomsById.getOrDefault(requestedRoom, roomsById.get("general"));
        if (!targetRoom.systemRoom && "private".equals(targetRoom.privacy)
                && !targetRoom.authorizedSessions.contains(session.getId())
                && !isRoomCreator(profile, targetRoom)) {
            targetRoom = roomsById.get("general");
        }

        if (targetRoom.bannedUsers.contains(rawName.toLowerCase(Locale.ROOT))) {
            sendError(session, "Estas baneado de la sala " + targetRoom.name + ".");
            return;
        }

        profile.roomId = targetRoom.id;
        targetRoom.authorizedSessions.add(session.getId());
        usersBySession.put(session.getId(), profile);

        ObjectNode ok = mapper.createObjectNode();
        ok.put("type", "REGISTERED");
        ok.put("sessionId", session.getId());
        ok.put("name", profile.name);
        ok.put("nationality", profile.nationality);
        ok.put("accessCode", profile.accessCode);
        ok.put("roomId", targetRoom.id);
        sendToSession(session, ok);

        ObjectNode joinNotice = mapper.createObjectNode();
        joinNotice.put("type", "SYSTEM_EVENT");
        joinNotice.put("roomId", targetRoom.id);
        joinNotice.put("text", profile.name + " (" + profile.nationality + ") entro a " + targetRoom.name + ".");
        broadcastToRoom(targetRoom.id, joinNotice);

        broadcastAllStates();
    }

    private void handleJoinRoom(WebSocketSession session, JsonNode root) {
        UserProfile user = usersBySession.get(session.getId());
        if (user == null) {
            sendError(session, "Primero debes ingresar con tu nombre, nacionalidad y codigo.");
            return;
        }

        String roomId = root.path("roomId").asText("").trim().toLowerCase(Locale.ROOT);
        ChatRoom room = roomsById.get(roomId);
        if (room == null) {
            sendError(session, "La sala seleccionada no existe.");
            return;
        }

        if (room.bannedUsers.contains(user.name.toLowerCase(Locale.ROOT))) {
            sendError(session, "Estas baneado de la sala " + room.name + ".");
            return;
        }

        if ("private".equals(room.privacy)
                && !room.authorizedSessions.contains(session.getId())
                && !isRoomCreator(user, room)) {
            sendError(session, "Esta sala es privada. Usa el codigo y el PIN de 4 digitos para ingresar.");
            return;
        }

        String oldRoomId = user.roomId;
        user.roomId = room.id;
        room.authorizedSessions.add(session.getId());

        if (oldRoomId != null && !oldRoomId.equals(room.id)) {
            ObjectNode leftMsg = mapper.createObjectNode();
            leftMsg.put("type", "SYSTEM_EVENT");
            leftMsg.put("roomId", oldRoomId);
            leftMsg.put("text", user.name + " cambio de sala.");
            broadcastToRoom(oldRoomId, leftMsg);
        }

        ObjectNode enteredMsg = mapper.createObjectNode();
        enteredMsg.put("type", "SYSTEM_EVENT");
        enteredMsg.put("roomId", room.id);
        enteredMsg.put("text", user.name + " (" + user.nationality + ") se unio a " + room.name + ".");
        broadcastToRoom(room.id, enteredMsg);

        broadcastAllStates();
    }

    private void handleJoinByCode(WebSocketSession session, JsonNode root) {
        UserProfile user = usersBySession.get(session.getId());
        if (user == null) {
            sendError(session, "Primero debes ingresar con tu nombre, nacionalidad y codigo.");
            return;
        }

        String code = root.path("code").asText("").trim().toUpperCase(Locale.ROOT);
        String pin = root.path("pin").asText("").trim();

        ChatRoom matched = null;
        for (ChatRoom r : roomsById.values()) {
            if (r.code.equalsIgnoreCase(code)) {
                matched = r;
                break;
            }
        }

        if (matched == null) {
            sendError(session, "No se encontro ninguna sala con el codigo " + code + ".");
            return;
        }

        if (matched.bannedUsers.contains(user.name.toLowerCase(Locale.ROOT))) {
            sendError(session, "Estas baneado de la sala " + matched.name + ".");
            return;
        }

        if ("private".equals(matched.privacy) && !isRoomCreator(user, matched) && !matched.pin.equals(pin)) {
            sendError(session, "Codigo de sala o PIN de 4 digitos incorrecto para la sala privada.");
            return;
        }

        String oldRoomId = user.roomId;
        matched.authorizedSessions.add(session.getId());
        user.roomId = matched.id;

        if (oldRoomId != null && !oldRoomId.equals(matched.id)) {
            ObjectNode leftMsg = mapper.createObjectNode();
            leftMsg.put("type", "SYSTEM_EVENT");
            leftMsg.put("roomId", oldRoomId);
            leftMsg.put("text", user.name + " cambio de sala.");
            broadcastToRoom(oldRoomId, leftMsg);
        }

        ObjectNode unlocked = mapper.createObjectNode();
        unlocked.put("type", "ROOM_UNLOCKED");
        unlocked.put("roomId", matched.id);
        unlocked.put("privacy", matched.privacy);
        sendToSession(session, unlocked);

        ObjectNode enteredMsg = mapper.createObjectNode();
        enteredMsg.put("type", "SYSTEM_EVENT");
        enteredMsg.put("roomId", matched.id);
        enteredMsg.put("text", user.name + " (" + user.nationality + ") entro a la sala.");
        broadcastToRoom(matched.id, enteredMsg);

        broadcastAllStates();
    }

    private void handleCreateRoom(WebSocketSession session, JsonNode root) {
        UserProfile creator = usersBySession.get(session.getId());
        if (creator == null) {
            sendError(session, "Primero debes ingresar al chat para crear una sala.");
            return;
        }

        String name = root.path("name").asText("").trim();
        String topic = root.path("topic").asText("").trim();
        String iconKey = root.path("iconKey").asText("gamepad").trim().toLowerCase(Locale.ROOT);
        String privacy = "private".equalsIgnoreCase(root.path("privacy").asText("public")) ? "private" : "public";
        String pin = root.path("pin").asText("").trim();

        if (name.length() < 2 || name.length() > 35) {
            sendError(session, "El nombre de la sala debe tener entre 2 y 35 caracteres.");
            return;
        }

        if ("private".equals(privacy) && !pin.matches("\\d{4}")) {
            sendError(session, "Las salas privadas requieren un PIN exacto de 4 digitos.");
            return;
        }

        String slug = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-");
        if (slug.isEmpty()) {
            slug = "sala";
        }
        String roomId = slug + "-" + (1000 + RANDOM.nextInt(9000));
        String code = "SALA-" + (1000 + RANDOM.nextInt(9000));

        ChatRoom room = new ChatRoom();
        room.id = roomId;
        room.name = name;
        room.topic = topic.isEmpty() ? "Sala personalizada" : topic;
        room.iconKey = iconKey;
        room.privacy = privacy;
        room.code = code;
        room.pin = "private".equals(privacy) ? pin : "";
        room.creatorSessionId = session.getId();
        room.creatorName = creator.name;
        room.creatorNationality = creator.nationality;
        room.creatorAccessCode = creator.accessCode;
        room.systemRoom = false;
        room.authorizedSessions.add(session.getId());

        roomsById.put(room.id, room);
        creator.roomId = room.id;

        ObjectNode created = mapper.createObjectNode();
        created.put("type", "ROOM_CREATED");
        created.put("roomId", room.id);
        created.put("roomName", room.name);
        created.put("code", room.code);
        created.put("privacy", room.privacy);
        created.put("pin", room.pin);
        sendToSession(session, created);

        ObjectNode welcome = mapper.createObjectNode();
        welcome.put("type", "SYSTEM_EVENT");
        welcome.put("roomId", room.id);
        welcome.put("text", creator.name + " creo la sala " + room.name + " (" + ("private".equals(room.privacy) ? "Privada" : "Publica") + ").");
        broadcastToRoom(room.id, welcome);

        broadcastAllStates();
    }

    private void handleEditRoom(WebSocketSession session, JsonNode root) {
        UserProfile actor = usersBySession.get(session.getId());
        if (actor == null) {
            sendError(session, "Accion no autorizada.");
            return;
        }

        String roomId = root.path("roomId").asText("").trim();
        ChatRoom room = roomsById.get(roomId);
        if (room == null) {
            sendError(session, "La sala no existe.");
            return;
        }

        if (!isRoomCreator(actor, room)) {
            sendError(session, "Solo el creador (con nombre, nacionalidad y codigo exactos) puede editar esta sala.");
            return;
        }

        String newName = root.path("name").asText("").trim();
        String newTopic = root.path("topic").asText("").trim();
        String newIconKey = root.path("iconKey").asText(room.iconKey).trim().toLowerCase(Locale.ROOT);
        String newPrivacy = "private".equalsIgnoreCase(root.path("privacy").asText(room.privacy)) ? "private" : "public";
        String newPin = root.path("pin").asText("").trim();

        if (newName.length() < 2 || newName.length() > 35) {
            sendError(session, "El nombre de la sala debe tener entre 2 y 35 caracteres.");
            return;
        }

        if ("private".equals(newPrivacy)) {
            String effectivePin = newPin.isEmpty() ? room.pin : newPin;
            if (!effectivePin.matches("\\d{4}")) {
                sendError(session, "Para que la sala sea privada debes asignar un PIN exacto de 4 digitos.");
                return;
            }
            room.pin = effectivePin;
        } else {
            room.pin = "";
        }

        room.name = newName;
        room.topic = newTopic.isEmpty() ? "Sala personalizada" : newTopic;
        room.iconKey = newIconKey;
        room.privacy = newPrivacy;

        ObjectNode notice = mapper.createObjectNode();
        notice.put("type", "SYSTEM_EVENT");
        notice.put("roomId", room.id);
        notice.put("text", actor.name + " actualizo la sala '" + room.name + "' (" + ("private".equals(room.privacy) ? "Privada" : "Publica") + ").");
        broadcastToRoom(room.id, notice);

        broadcastAllStates();
    }

    private void handleDeleteRoom(WebSocketSession session, JsonNode root) {
        UserProfile actor = usersBySession.get(session.getId());
        if (actor == null) {
            sendError(session, "Accion no autorizada.");
            return;
        }

        String roomId = root.path("roomId").asText("").trim();
        ChatRoom room = roomsById.get(roomId);
        if (room == null) {
            sendError(session, "La sala no existe.");
            return;
        }

        if (!isRoomCreator(actor, room)) {
            sendError(session, "Solo el creador (con nombre, nacionalidad y codigo exactos) puede eliminar esta sala.");
            return;
        }

        roomsById.remove(room.id);

        for (UserProfile u : usersBySession.values()) {
            if (room.id.equals(u.roomId)) {
                u.roomId = "general";
                WebSocketSession uWs = findSession(u.sessionId);
                if (uWs != null) {
                    ObjectNode moved = mapper.createObjectNode();
                    moved.put("type", "ROOM_DELETED_NOTICE");
                    moved.put("roomName", room.name);
                    moved.put("text", "La sala '" + room.name + "' fue eliminada por su creador. Volviste a General.");
                    sendToSession(uWs, moved);
                }
            }
        }

        broadcastAllStates();
    }

    private void handleChatMessage(WebSocketSession session, JsonNode root) {
        UserProfile sender = usersBySession.get(session.getId());
        if (sender == null) {
            sendError(session, "Debes registrarte antes de enviar mensajes.");
            return;
        }

        String roomId = root.path("roomId").asText(sender.roomId).trim().toLowerCase(Locale.ROOT);
        ChatRoom room = roomsById.get(roomId);
        if (room == null) {
            sendError(session, "La sala no existe.");
            return;
        }

        if (room.bannedUsers.contains(sender.name.toLowerCase(Locale.ROOT))) {
            sendError(session, "No puedes enviar mensajes porque estas baneado de esta sala.");
            return;
        }

        String content = root.path("content").asText("").trim();
        String gifUrl = root.path("gifUrl").asText("").trim();
        String kind = "gif".equalsIgnoreCase(root.path("kind").asText("text")) && !gifUrl.isEmpty() ? "gif" : "text";

        if ("text".equals(kind) && content.isEmpty()) {
            return;
        }

        ObjectNode outbound = mapper.createObjectNode();
        outbound.put("type", "CHAT_MESSAGE");
        outbound.put("senderSessionId", session.getId());
        outbound.put("senderName", sender.name);
        outbound.put("senderNationality", sender.nationality);
        outbound.put("roomId", room.id);
        outbound.put("roomName", room.name);
        outbound.put("kind", kind);
        outbound.put("content", content);
        outbound.put("gifUrl", gifUrl);
        outbound.put("time", LocalTime.now().format(TIME_FMT));

        broadcastToRoom(room.id, outbound);
    }

    private void handleKickUser(WebSocketSession session, JsonNode root) {
        UserProfile actor = usersBySession.get(session.getId());
        if (actor == null) {
            sendError(session, "Accion no autorizada.");
            return;
        }

        String targetSessionId = root.path("targetSessionId").asText("");
        String roomId = root.path("roomId").asText(actor.roomId).trim();

        if (targetSessionId.equals(session.getId())) {
            sendError(session, "No puedes eliminarte a ti mismo.");
            return;
        }

        UserProfile target = usersBySession.get(targetSessionId);
        ChatRoom room = roomsById.get(roomId);
        if (target == null || room == null) {
            sendError(session, "Usuario o sala no encontrados.");
            return;
        }

        if (!isRoomCreator(actor, room)) {
            sendError(session, "Solo el creador de la sala puede eliminar integrantes.");
            return;
        }

        room.authorizedSessions.remove(target.sessionId);
        target.roomId = "general";

        WebSocketSession targetWs = findSession(target.sessionId);
        if (targetWs != null) {
            ObjectNode kickedNotice = mapper.createObjectNode();
            kickedNotice.put("type", "KICKED_NOTICE");
            kickedNotice.put("roomId", room.id);
            kickedNotice.put("roomName", room.name);
            kickedNotice.put("reason", actor.name + " te elimino de la sala " + room.name + ".");
            sendToSession(targetWs, kickedNotice);
        }

        ObjectNode alert = mapper.createObjectNode();
        alert.put("type", "SYSTEM_EVENT");
        alert.put("roomId", room.id);
        alert.put("text", target.name + " fue eliminado de la sala por " + actor.name + ".");
        broadcastToRoom(room.id, alert);

        broadcastAllStates();
    }

    private void handleBanUser(WebSocketSession session, JsonNode root) {
        UserProfile actor = usersBySession.get(session.getId());
        if (actor == null) {
            sendError(session, "Accion no autorizada.");
            return;
        }

        String targetSessionId = root.path("targetSessionId").asText("");
        String roomId = root.path("roomId").asText(actor.roomId).trim();

        if (targetSessionId.equals(session.getId())) {
            sendError(session, "No puedes banearte a ti mismo.");
            return;
        }

        UserProfile target = usersBySession.get(targetSessionId);
        ChatRoom room = roomsById.get(roomId);
        if (target == null || room == null) {
            sendError(session, "Usuario o sala no encontrados para aplicar el baneo.");
            return;
        }

        if (!isRoomCreator(actor, room)) {
            sendError(session, "Solo el creador de la sala puede banear integrantes aqui.");
            return;
        }

        room.bannedUsers.add(target.name.toLowerCase(Locale.ROOT));
        room.authorizedSessions.remove(target.sessionId);
        target.roomId = "general";

        WebSocketSession targetWs = findSession(target.sessionId);
        if (targetWs != null) {
            ObjectNode bannedNotice = mapper.createObjectNode();
            bannedNotice.put("type", "BANNED_NOTICE");
            bannedNotice.put("roomId", room.id);
            bannedNotice.put("roomName", room.name);
            bannedNotice.put("reason", "Has sido baneado de la sala " + room.name + " por " + actor.name + ".");
            sendToSession(targetWs, bannedNotice);
        }

        ObjectNode roomAlert = mapper.createObjectNode();
        roomAlert.put("type", "SYSTEM_EVENT");
        roomAlert.put("roomId", room.id);
        roomAlert.put("text", target.name + " fue baneado de la sala por " + actor.name + ".");
        broadcastToRoom(room.id, roomAlert);

        broadcastAllStates();
    }

    private void sendStateToSession(WebSocketSession session) {
        sendToSession(session, buildStatePayloadFor(session.getId()));
    }

    private void broadcastAllStates() {
        for (WebSocketSession s : sessions) {
            if (s.isOpen()) {
                sendToSession(s, buildStatePayloadFor(s.getId()));
            }
        }
    }

    private ObjectNode buildStatePayloadFor(String viewerSessionId) {
        ObjectNode root = mapper.createObjectNode();
        root.put("type", "STATE_SYNC");
        root.put("totalOnline", usersBySession.size());

        UserProfile viewer = usersBySession.get(viewerSessionId);

        ArrayNode roomsArr = mapper.createArrayNode();
        String[] order = {"general", "deportes", "musica"};
        for (String fixedId : order) {
            ChatRoom r = roomsById.get(fixedId);
            if (r != null) {
                roomsArr.add(buildRoomNode(r, viewer));
            }
        }
        for (ChatRoom r : roomsById.values()) {
            if (!r.systemRoom) {
                boolean isPrivate = "private".equals(r.privacy);
                boolean hasAccess = r.authorizedSessions.contains(viewerSessionId) || isRoomCreator(viewer, r);
                if (!isPrivate || hasAccess) {
                    roomsArr.add(buildRoomNode(r, viewer));
                }
            }
        }
        root.set("rooms", roomsArr);

        ArrayNode usersArr = mapper.createArrayNode();
        for (UserProfile u : usersBySession.values()) {
            ObjectNode uNode = mapper.createObjectNode();
            uNode.put("sessionId", u.sessionId);
            uNode.put("name", u.name);
            uNode.put("nationality", u.nationality);
            uNode.put("roomId", u.roomId);
            usersArr.add(uNode);
        }
        root.set("users", usersArr);

        return root;
    }

    private ObjectNode buildRoomNode(ChatRoom r, UserProfile viewer) {
        int count = 0;
        for (UserProfile u : usersBySession.values()) {
            if (u.roomId.equals(r.id)) {
                count++;
            }
        }
        boolean creator = isRoomCreator(viewer, r);
        ObjectNode rNode = mapper.createObjectNode();
        rNode.put("id", r.id);
        rNode.put("name", r.name);
        rNode.put("topic", r.topic);
        rNode.put("iconKey", r.iconKey);
        rNode.put("privacy", r.privacy);
        rNode.put("code", r.code);
        rNode.put("pin", creator ? r.pin : "");
        rNode.put("systemRoom", r.systemRoom);
        rNode.put("creatorName", r.creatorName);
        rNode.put("isCreator", creator);
        rNode.put("onlineCount", count);
        return rNode;
    }

    private void broadcastToRoom(String roomId, ObjectNode payload) {
        String json;
        try {
            json = mapper.writeValueAsString(payload);
        } catch (Exception e) {
            return;
        }
        TextMessage msg = new TextMessage(json);
        for (WebSocketSession s : sessions) {
            if (!s.isOpen()) {
                continue;
            }
            UserProfile u = usersBySession.get(s.getId());
            if (u != null && roomId.equals(u.roomId)) {
                try {
                    s.sendMessage(msg);
                } catch (IOException ignored) {
                }
            }
        }
    }

    private void sendToSession(WebSocketSession session, ObjectNode payload) {
        if (session == null || !session.isOpen()) {
            return;
        }
        try {
            session.sendMessage(new TextMessage(mapper.writeValueAsString(payload)));
        } catch (IOException ignored) {
        }
    }

    private void sendError(WebSocketSession session, String message) {
        ObjectNode err = mapper.createObjectNode();
        err.put("type", "ERROR");
        err.put("message", message);
        sendToSession(session, err);
    }

    private WebSocketSession findSession(String sessionId) {
        for (WebSocketSession s : sessions) {
            if (s.getId().equals(sessionId)) {
                return s;
            }
        }
        return null;
    }

    private static class UserProfile {
        String sessionId;
        String name;
        String nationality;
        String accessCode;
        String roomId;
    }

    private static class ChatRoom {
        String id;
        String name;
        String topic;
        String iconKey;
        String privacy;
        String code;
        String pin;
        String creatorSessionId;
        String creatorName;
        String creatorNationality;
        String creatorAccessCode;
        boolean systemRoom;
        Set<String> authorizedSessions = new CopyOnWriteArraySet<>();
        Set<String> bannedUsers = new CopyOnWriteArraySet<>();
    }
}