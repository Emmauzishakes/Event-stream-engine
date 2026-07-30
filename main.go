package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// 1. Upgraded EventRoom tracks Viewers, Likes, Tracks, Timer, and Chat History
type EventRoom struct {
	VideoTrack   *webrtc.TrackLocalStaticRTP
	AudioTrack   *webrtc.TrackLocalStaticRTP
	Clients      map[*websocket.Conn]bool
	Broadcaster  *websocket.Conn
	ViewerCount  int
	TotalLikes   int
	BlockedUsers map[string]bool
	StartTime    int64
	ChatHistory  []SignalingMessage
	sync.RWMutex
}

var (
	roomsLock sync.RWMutex
	rooms     = make(map[string]*EventRoom)
)

// Helper method to safely append chat history under a write lock
func (r *EventRoom) AddToChatHistory(msg SignalingMessage) {
	if msg.User == "SYSTEM_COMMAND" {
		return
	}
	r.Lock()
	defer r.Unlock()
	r.ChatHistory = append(r.ChatHistory, msg)
	if len(r.ChatHistory) > 20 {
		r.ChatHistory = r.ChatHistory[1:]
	}
}

// 2. Upgraded SignalingMessage handles stats, timer, and chat history
type SignalingMessage struct {
	EventSlug   string                    `json:"event_slug"`
	Type        string                    `json:"type"`
	SDP         webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate   *webrtc.ICECandidateInit  `json:"candidate,omitempty"`
	User        string                    `json:"user,omitempty"`
	Text        string                    `json:"text,omitempty"`
	Time        string                    `json:"time,omitempty"`
	LikeCount   int                       `json:"like_count,omitempty"`   // Incoming batched likes
	ViewerCount int                       `json:"viewer_count,omitempty"` // Outgoing stats
	TotalLikes  int                       `json:"total_likes,omitempty"`  // Outgoing stats
	Action      string                    `json:"action,omitempty"`
	MessageID   string                    `json:"message_id,omitempty"`
	Status      bool                      `json:"status,omitempty"`
	StartedAt   int64                     `json:"started_at,omitempty"`
	ChatHistory []SignalingMessage        `json:"chat_history,omitempty"`
}

func main() {
	http.HandleFunc("/ws", handleWebSocket)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("Live Go Streaming Engine running on :%s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return
	}

	var sigMsg SignalingMessage
	if err := json.Unmarshal(msg, &sigMsg); err != nil {
		return
	}

	// 1. Lock Pion to the exact UDP port range allowed by AWS EC2 Security Group
	settingEngine := webrtc.SettingEngine{}
	if err := settingEngine.SetEphemeralUDPPortRange(10000, 50000); err != nil {
		log.Printf("⚠️ Error setting UDP port range: %v", err)
	}

	// 2. Instantiate custom API with setting engine
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	peerConnection, err := api.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
			{
				URLs: []string{
					"turn:openrelay.metered.ca:80",
					"turn:openrelay.metered.ca:443",
					"turn:openrelay.metered.ca:443?transport=tcp",
				},
				Username:   "openrelayproject",
				Credential: "openrelayproject",
			},
		},
	})
	if err != nil {
		return
	}
	defer peerConnection.Close()

	switch sigMsg.Type {
	case "broadcaster":
		handleBroadcaster(conn, peerConnection, sigMsg)
	case "viewer":
		handleViewer(conn, peerConnection, sigMsg)
	default:
		log.Println("Unknown connection type:", sigMsg.Type)
	}
}

// --- BROADCASTER LOGIC ---
func handleBroadcaster(conn *websocket.Conn, pc *webrtc.PeerConnection, sigMsg SignalingMessage) {
	log.Printf("📹 Broadcaster connected for event: %s", sigMsg.EventSlug)

	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		localTrack, err := webrtc.NewTrackLocalStaticRTP(track.Codec().RTPCodecCapability, track.ID(), track.StreamID())
		if err != nil {
			return
		}

		roomsLock.Lock()
		room, exists := rooms[sigMsg.EventSlug]
		if !exists {
			room = &EventRoom{
				StartTime:    time.Now().UnixMilli(),
				ChatHistory:  make([]SignalingMessage, 0, 20),
				Clients:      make(map[*websocket.Conn]bool),
				BlockedUsers: make(map[string]bool),
			}
			rooms[sigMsg.EventSlug] = room
		}

		room.Broadcaster = conn

		if track.Kind() == webrtc.RTPCodecTypeVideo {
			room.VideoTrack = localTrack
		} else if track.Kind() == webrtc.RTPCodecTypeAudio {
			room.AudioTrack = localTrack
		}

		statsMsg := SignalingMessage{
			Type:        "stats",
			StartedAt:   room.StartTime,
			ViewerCount: room.ViewerCount,
			TotalLikes:  room.TotalLikes,
			ChatHistory: room.ChatHistory,
		}
		roomsLock.Unlock()

		conn.WriteJSON(statsMsg)

		go func() {
			rtpBuf := make([]byte, 1400)
			for {
				i, _, err := track.Read(rtpBuf)
				if err != nil {
					return
				}
				if _, err := localTrack.Write(rtpBuf[:i]); err != nil {
					return
				}
			}
		}()
	})

	if err := pc.SetRemoteDescription(sigMsg.SDP); err != nil {
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return
	}
	pc.SetLocalDescription(answer)
	conn.WriteJSON(answer)

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			conn.WriteJSON(map[string]interface{}{"candidate": c.ToJSON()})
		}
	})

	// CONTINUOUS LISTENING LOOP
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var update SignalingMessage
		if err := json.Unmarshal(msg, &update); err == nil {
			if update.Candidate != nil {
				pc.AddICECandidate(*update.Candidate)
			}

			if update.Type == "chat" || update.Type == "moderation" {
				roomsLock.RLock()
				room, exists := rooms[sigMsg.EventSlug]
				roomsLock.RUnlock()

				if exists {
					if update.Type == "chat" {
						room.AddToChatHistory(update)
					}

					if update.Type == "moderation" {
						room.AddToChatHistory(update)
						room.Lock()
						if room.BlockedUsers == nil {
							room.BlockedUsers = make(map[string]bool)
						}
						if update.Action == "block_user" {
							room.BlockedUsers[update.User] = true
						} else if update.Action == "unblock_user" {
							room.BlockedUsers[update.User] = false
						}
						room.Unlock()
					}

					room.RLock()
					for client := range room.Clients {
						client.WriteJSON(update)
					}
					room.RUnlock()
				}
			}
		}
	}
}

// --- VIEWER LOGIC ---
func handleViewer(conn *websocket.Conn, pc *webrtc.PeerConnection, sigMsg SignalingMessage) {
	log.Printf("👀 Viewer connected for event: %s", sigMsg.EventSlug)

	roomsLock.RLock()
	room, exists := rooms[sigMsg.EventSlug]
	roomsLock.RUnlock()

	if !exists || (room.VideoTrack == nil && room.AudioTrack == nil) {
		conn.WriteJSON(map[string]string{"error": "no_active_broadcast"})
		return
	}

	if room.VideoTrack != nil {
		pc.AddTrack(room.VideoTrack)
	}
	if room.AudioTrack != nil {
		pc.AddTrack(room.AudioTrack)
	}

	if err := pc.SetRemoteDescription(sigMsg.SDP); err != nil {
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return
	}
	pc.SetLocalDescription(answer)
	conn.WriteJSON(answer)

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			conn.WriteJSON(map[string]interface{}{"candidate": c.ToJSON()})
		}
	})

	// Register client and update Viewer Count
	room.Lock()
	if room.Clients == nil {
		room.Clients = make(map[*websocket.Conn]bool)
	}
	room.Clients[conn] = true
	room.ViewerCount++
	currentViewers := room.ViewerCount
	currentLikes := room.TotalLikes
	startedAt := room.StartTime

	chatHistory := make([]SignalingMessage, len(room.ChatHistory))
	copy(chatHistory, room.ChatHistory)
	room.Unlock()

	// Send initial state to the new viewer, and broadcast new viewer count to everyone
	statsMsg := SignalingMessage{
		Type:        "stats",
		ViewerCount: currentViewers,
		TotalLikes:  currentLikes,
		StartedAt:   startedAt,
		ChatHistory: chatHistory,
	}
	conn.WriteJSON(statsMsg)

	room.RLock()
	for client := range room.Clients {
		if client != conn {
			client.WriteJSON(statsMsg)
		}
	}
	room.RUnlock()

	// Unregister client on disconnect
	defer func() {
		room.Lock()
		delete(room.Clients, conn)
		room.ViewerCount--
		newViewers := room.ViewerCount
		room.Unlock()

		// Broadcast that a viewer left
		room.RLock()
		leaveMsg := SignalingMessage{Type: "stats", ViewerCount: newViewers, TotalLikes: room.TotalLikes}
		for client := range room.Clients {
			client.WriteJSON(leaveMsg)
		}
		room.RUnlock()
	}()

	// CONTINUOUS LISTENING LOOP
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var update SignalingMessage
		if err := json.Unmarshal(msg, &update); err == nil {
			if update.Candidate != nil {
				pc.AddICECandidate(*update.Candidate)
			}

			// Handle Chat
			if update.Type == "chat" {
				room.AddToChatHistory(update)

				room.RLock()
				isBlocked := false
				if room.BlockedUsers != nil {
					isBlocked = room.BlockedUsers[update.User]
				}
				room.RUnlock()

				if isBlocked {
					continue
				}

				room.RLock()
				for client := range room.Clients {
					client.WriteJSON(update)
				}
				if room.Broadcaster != nil {
					room.Broadcaster.WriteJSON(update)
				}
				room.RUnlock()
			}

			// Handle Batched Likes
			if update.Type == "like" {
				room.Lock()
				if update.LikeCount > 0 {
					room.TotalLikes += update.LikeCount
				} else {
					room.TotalLikes++
				}
				newLikes := room.TotalLikes
				newViewers := room.ViewerCount
				room.Unlock()

				// Broadcast the new like total
				room.RLock()
				likeMsg := SignalingMessage{Type: "stats", ViewerCount: newViewers, TotalLikes: newLikes}
				for client := range room.Clients {
					client.WriteJSON(likeMsg)
				}
				if room.Broadcaster != nil {
					room.Broadcaster.WriteJSON(likeMsg)
				}
				room.RUnlock()
			}
		}
	}
}