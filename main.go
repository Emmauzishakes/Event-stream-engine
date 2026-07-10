package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// 1. Upgraded EventRoom tracks Viewers and Likes
type EventRoom struct {
	VideoTrack  *webrtc.TrackLocalStaticRTP
	AudioTrack  *webrtc.TrackLocalStaticRTP
	Clients     map[*websocket.Conn]bool
	ViewerCount int
	TotalLikes  int
	sync.RWMutex
}

var (
	roomsLock sync.RWMutex
	rooms     = make(map[string]*EventRoom)
)

// 2. Upgraded SignalingMessage handles stats and batched likes
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
}

func main() {
	http.HandleFunc("/ws", handleWebSocket)
	fmt.Println("Live Go Streaming Engine running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
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

	peerConnection, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
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
			room = &EventRoom{}
			rooms[sigMsg.EventSlug] = room
		}
		if track.Kind() == webrtc.RTPCodecTypeVideo {
			room.VideoTrack = localTrack
		} else if track.Kind() == webrtc.RTPCodecTypeAudio {
			room.AudioTrack = localTrack
		}
		roomsLock.Unlock()

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

	// 3. Register client and update Viewer Count
	room.Lock()
	if room.Clients == nil {
		room.Clients = make(map[*websocket.Conn]bool)
	}
	room.Clients[conn] = true
	room.ViewerCount++
	currentViewers := room.ViewerCount
	currentLikes := room.TotalLikes
	room.Unlock()

	// 4. Send initial state to the new viewer, and broadcast new viewer count to everyone
	statsMsg := SignalingMessage{Type: "stats", ViewerCount: currentViewers, TotalLikes: currentLikes}
	conn.WriteJSON(statsMsg)

	room.RLock()
	for client := range room.Clients {
		if client != conn {
			client.WriteJSON(statsMsg)
		}
	}
	room.RUnlock()

	// 5. Unregister client on disconnect
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

			// 6. Handle Chat
			if update.Type == "chat" {
				room.RLock()
				for client := range room.Clients {
					client.WriteJSON(update)
				}
				room.RUnlock()
			}

			// 7. Handle Batched Likes
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
				room.RUnlock()
			}
		}
	}
}
