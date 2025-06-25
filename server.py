from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uuid
import json
import random
import logging
import asyncio
import redis.asyncio as redis
import os
from dotenv import load_dotenv

load_dotenv()  # .env を読み込む

redis_url = os.getenv("REDIS_URL")
rdb = redis.from_url(redis_url, decode_responses=True)

logging.basicConfig(level=logging.INFO)

app = FastAPI()


connected_sockets = {}

def save_board():
    board = [[0] * 8 for _ in range(8)]
    mid = 4
    board[mid - 1][mid - 1] = -1
    board[mid][mid] = -1
    board[mid - 1][mid] = 1
    board[mid][mid - 1] = 1
    return board

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    logging.info("[CONNECT] WebSocket 接続開始")
            # 再接続時に盤面・ターンを復元送信
    
    await websocket.accept()
    user_id=None

    try:
        init_message = await websocket.receive_text()
        logging.info(f"[DEBUG] 初期メッセージ受信: {init_message}")
        init_data = json.loads(init_message)
        logging.info(f"[DEBUG] 初期データ: {init_data}")
        data_type = init_data.get("type")

        if data_type == "register":
            user_id = init_data.get("user_id")
            name = init_data.get("name")
            connected_sockets[user_id] = websocket

            logging.info(f"[REGISTER] user_id={user_id}, name={name} が接続しました")

            exists = await rdb.exists(f"user:{user_id}")
            if exists:
                status = await rdb.hget(f"user:{user_id}", "status")
                logging.info(f"[REGISTER] Redisに既存 user:{user_id}（status={status}）")
                if status == "matched":

                    game_id = await rdb.hget(f"user:{user_id}", "game_id")

                    board_data = await rdb.get(f"board:{game_id}")
                    turn = await rdb.get(f"turn:{game_id}")
                    color = await rdb.hget(f"user:{user_id}", "color")
                    opponent_id = await rdb.hget(f"user:{user_id}", "opponent")
                    your_turn = (turn == color)

                    opponent_name = await rdb.hget(f"user:{opponent_id}", "name") if opponent_id else None

                    if board_data and turn and color:
                        logging.info(f"[RESTORE] user_id={user_id}, turn={turn}, color={color}")
                        await websocket.send_text(json.dumps({
                            "type": "restore_board",
                            "board": json.loads(board_data),
                            "current_player": turn,
                            "your_color": 1 if color == "black" else -1,
                            "your_turn": your_turn,
                            "opponent_name": opponent_name,
                            "reconnect_code": True
                        }))
                        logging.info(f"[RESTORE] Sent restore_board to {user_id}")

                    # 相手に通知
                        if opponent_id in connected_sockets:
                            try:
                                await connected_sockets[opponent_id].send_text(json.dumps({
                                    "type": "opponent_reconnected",
                                    "user_id": user_id
                                }))
                                logging.info(f"[RESTORE] Notified opponent {opponent_id}")
                            
                            # 最新盤面を相手にも送る
                                opponent_turn = await rdb.get(f"turn:{game_id}")
                                opponent_board_data = await rdb.get(f"board:{game_id}")
                                opponent_color = await rdb.hget(f"user:{opponent_id}", "color")

                                if opponent_turn and opponent_board_data:
                                    current_player_value = 1 if opponent_turn == opponent_color else -1
                                    await connected_sockets[opponent_id].send_text(json.dumps({
                                        "type": "update_board",
                                        "board": json.loads(opponent_board_data),
                                        "current_player": current_player_value,
                                    }))
                            except Exception as e:
                                logging.info(f"[WARN] Failed to notify opponent: {e}")
                    else:
                        logging.info(f"[RESTORE] board_data などが不完全")

                else:
                # 🆕 初回接続と判定 → Redis に登録
                    logging.info(f"[REGISTER] user_id={user_id} はstatus={status}のため、マッチング待機に復帰")
                    await rdb.hset(f"user:{user_id}", mapping={
                        "name": name,
                        "status": "waiting",
                        "opponent": ""
                    })
                    asyncio.create_task(try_match(user_id))
            else:
                
                # 🆕 初回接続と判定 → Redis に登録
                logging.info(f"[REGISTER] user_id={user_id} は新規登録と判断")
                await rdb.hset(f"user:{user_id}", mapping={
                    "name": name,
                    "status": "waiting",
                    "opponent": ""
                })
                asyncio.create_task(try_match(user_id))

   
        
        
        while True:
         try:
            message = await websocket.receive_text()
            data = json.loads(message)


            if data.get("type") == "move":
                x = data["x"]
                y = data["y"]

                opponent_id = await rdb.hget(f"user:{user_id}", "opponent")
                my_color = await rdb.hget(f"user:{user_id}", "color")
                opponent_color = "black" if my_color == "white" else "white"

    # 現在の board を取得し、反転処理
                game_id = await rdb.hget(f"user:{user_id}", "game_id")
                board_data = await rdb.get(f"board:{game_id}")
                board = json.loads(board_data) if board_data else [[0]*8 for _ in range(8)]
                color_value = 1 if my_color == "black" else -1

    # 石を置いて、反転処理を実行
                def place_stone(board, x, y, color):
                    directions = [(-1, -1), (-1, 0), (-1, 1),
                                  (0, -1),          (0, 1),
                                  (1, -1),  (1, 0), (1, 1)]
                    flipped = []

                    if board[x][y] != 0:
                        return board  # 無効な位置

                    board[x][y] = color
                    for dx, dy in directions:
                        nx, ny = x + dx, y + dy
                        temp = []
                        while 0 <= nx < 8 and 0 <= ny < 8 and board[nx][ny] == -color:
                            temp.append((nx, ny))
                            nx += dx
                            ny += dy
                        if 0 <= nx < 8 and 0 <= ny < 8 and board[nx][ny] == color:
                            for fx, fy in temp:
                                board[fx][fy] = color
                                flipped.append((fx, fy))
                    return board

                board = place_stone(board, x, y, color_value)

    # 次のターンを決定
                current_turn = await rdb.get(f"turn:{game_id}")
                if not current_turn:
                    current_turn = "black"
                next_turn = "white" if current_turn == "black" else "black"

    # Redisに保存（再接続対応）
                await rdb.set(f"board:{game_id}", json.dumps(board), ex=3600)
                
                await rdb.set(f"turn:{game_id}", next_turn, ex=3600)
               

    # 両者に座標と色、次のターンを通知（board は送らない）
                for uid, color, socket in [
                    (user_id, my_color, connected_sockets.get(user_id)),
                    (opponent_id, opponent_color, connected_sockets.get(opponent_id))
                ]:
                    if socket:
                        await socket.send_text(json.dumps({
                            "type": "move",
                            "x": x,
                            "y": y,
                            "color": my_color,
                            "next_turn": next_turn,
                            "your_color": "black" if my_color == 1 else "white",
                            "your_turn": (next_turn == color)
                        }))

            elif data.get("type") == "pass":
                game_id = await rdb.hget(f"user:{user_id}", "game_id")
                opponent_id = await rdb.hget(f"user:{user_id}", "opponent")

    # 現在の board を取得
                board_data = await rdb.get(f"board:{game_id}")
                board = json.loads(board_data) if board_data else [[0]*8 for _ in range(8)]

    # パス回数記録
                my_passed = await rdb.get(f"pass:{user_id}")
                opponent_passed = await rdb.get(f"pass:{opponent_id}")

                if my_passed == "true" and opponent_passed == "true":
                    print("[INFO] 両者が連続でパスしました。ゲーム終了処理を開始します。")
                    for uid in [user_id, opponent_id]:
                        if uid in connected_sockets:
                            await connected_sockets[uid].send_text(json.dumps({
                                "type": "end_game",
                                "board": board,
                                "your_color": await rdb.hget(f"user:{uid}", "color")
                            }))
                    return
                await rdb.set(f"pass:{user_id}", "true", ex=40)

                current_turn = await rdb.get(f"turn:{game_id}")
                if not current_turn:
                    current_turn = "black"
                next_turn = "white" if current_turn == "black" else "black"

    # 保存（再接続用）
                await rdb.set(f"board:{game_id}", json.dumps(board), ex=3600)
                
                await rdb.set(f"turn:{game_id}", next_turn, ex=3600)
                
    # 相手にパス通知
                opponent_color = await rdb.hget(f"user:{opponent_id}", "color")
                if opponent_id in connected_sockets:
                    await connected_sockets[opponent_id].send_text(json.dumps({
                        "type": "pass",
                        "next_turn": next_turn,
                        "your_color": opponent_color,
                        "your_turn": (next_turn == opponent_color)
            }))
       
                    
            elif data.get("type") == "end_game":
                opponent_id = await rdb.hget(f"user:{user_id}", "opponent")
                game_id = await rdb.hget(f"user:{user_id}", "game_id")

    # 再接続用に有効期限を延長
                await rdb.expire(f"board:{game_id}", 40)
                
                await rdb.expire(f"turn:{game_id}", 40)
                
    # Redisから取得（受信ではなく）
                board_data = await rdb.get(f"board:{game_id}")
                turn = await rdb.get(f"turn:{game_id}")
                board = json.loads(board_data) if board_data else [[0]*8 for _ in range(8)]

                my_color = await rdb.hget(f"user:{user_id}", "color")
                opponent_color = "black" if my_color == "white" else "white"

    # 状態を waiting に戻す
                await rdb.hset(f"user:{user_id}", mapping={"status": "waiting", "opponent": ""})
                await rdb.hset(f"user:{opponent_id}", mapping={"status": "waiting", "opponent": ""})

                for uid, color, socket in [
                     (user_id, my_color, connected_sockets.get(user_id)),
                     (opponent_id, opponent_color, connected_sockets.get(opponent_id))
                ]:
                    if socket:
                        try:
                            opponent_name = await rdb.hget(f"user:{opponent_id if uid == user_id else user_id}", "name")
                            await socket.send_text(json.dumps({
                                "type": "end_game",
                                "board": board,
                                "current_player": 1 if turn == "black" else -1,
                                "your_color": color,
                                "opponent_name":opponent_name
                            }))
                        except Exception as e:
                            logging.warning(f"[WARN] end_game 送信失敗: {e}")

                
         except WebSocketDisconnect:
            logging.info(f"[DISCONNECT] {user_id} が切断されました")
            if user_id:
                 await handle_disconnect(user_id)
            break
    except Exception as e:
        logging.warning(f"[WARN] 通常ループ中のエラー: {e}")

async def try_match(current_id):
    logging.info(f"[DEBUG] try_match called for {current_id}")
    
    
    all_keys = await rdb.keys("user:*")
    waiting_users = []
    for key in all_keys:
        uid = key.split(":")[1]
        status = await rdb.hget(key, "status")
        if status == "waiting":
            waiting_users.append(uid)

    logging.info(f"[DEBUG] waiting_users =", waiting_users)

    if len(waiting_users) < 2:
        return

    # 2人をランダムに選ぶ
    candidates = [uid for uid in waiting_users if uid != current_id]
    if not candidates:
        return
    
    
    opponent_id = random.choice(candidates)
    user_ids = [current_id, opponent_id]
    random.shuffle(user_ids)
    user1_id, user2_id = user_ids[0], user_ids[1]

    user1_name = await rdb.hget(f"user:{user1_id}", "name")
    user2_name = await rdb.hget(f"user:{user2_id}", "name")

    
    user1_color = "black"
    user2_color = "white"
    first_turn = "black"
    
    
    game_id = str(uuid.uuid4())

    await rdb.hset(f"user:{user1_id}", mapping={
            "game_id": game_id,
            "status": "matched",
            "opponent": user2_id,
            "color": user1_color,
            "opponent_name": user2_name
    })
    await rdb.hset(f"user:{user2_id}", mapping={
            "game_id": game_id,
            "status": "matched",
            "opponent": user1_id,
            "color": user2_color,
            "opponent_name": user1_name
    })

    logging.info(f"[MATCH] {user1_id} ({user1_color}) vs {user2_id} ({user2_color})")

    await asyncio.sleep(2.0)

    board = save_board()

    if user1_id in connected_sockets:
        await connected_sockets[user1_id].send_text(json.dumps({
            "type": "start_game",
            "your_color": user1_color,
            "opponent_name": user2_name,
            "first_turn": first_turn,
            "board": board
        }))
    else:
        logging.warning(f"[try_match] user1_id {user1_id} がconnected_socketsに存在しません")

    if user2_id in connected_sockets:
        await connected_sockets[user2_id].send_text(json.dumps({
            "type": "start_game",
            "your_color": user2_color,
            "opponent_name": user1_name,
            "first_turn": first_turn,
            "board": board
        }))
    else:
         logging.warning(f"[try_match] user2_id {user2_id} がconnected_socketsに存在しません")

    

    await rdb.set(f"board:{game_id}", json.dumps(board), ex=3600)
    
    await rdb.set(f"turn:{game_id}", first_turn, ex=3600)
    

async def handle_disconnect(user_id):
    
    game_id = await rdb.hget(f"user:{user_id}", "game_id")
    opponent_id = await rdb.hget(f"user:{user_id}", "opponent")

    # userデータを完全には消さず、40秒だけ保持
    await rdb.expire(f"user:{user_id}", 40)
    await rdb.expire(f"board:{game_id}", 40)
    await rdb.expire(f"turn:{game_id}", 40)

    connected_sockets.pop(user_id, None)

    if opponent_id and opponent_id in connected_sockets:
        try:
            await connected_sockets[opponent_id].send_text(json.dumps({
                "type": "opponent_disconnected"
            }))
        except:
            pass

        # 対戦相手も40秒後にクリーンアップできるように更新
        await rdb.expire(f"user:{opponent_id}", 40)
        await rdb.expire(f"board:{game_id}", 40)
        await rdb.expire(f"turn:{game_id}", 40)

        asyncio.create_task(wait_end(user_id, opponent_id))

async def wait_end(disconnect_id, opponent_id):
    await asyncio.sleep(40)
    if disconnect_id not in connected_sockets:
        print(f"[TIMEOUT]ユーザー {disconnect_id} が再接続しませんでした。")
        game_id = await rdb.hget(f"user:{disconnect_id}", "game_id")
        
        board_data = await rdb.get(f"board:{game_id}")
        turn = await rdb.get(f"turn:{game_id}")
        color = await rdb.hget(f"user:{opponent_id}", "color")

        if board_data and turn and color and opponent_id in connected_sockets:
            try:
                await connected_sockets[opponent_id].send_text(json.dumps({
                    "type": "end_game",
                    "board": json.loads(board_data),
                    "current_player": 1 if turn == "black" else -1,
                    "your_color": color,
                    
                }))
                logging.info(f"[END_GAME] {opponent_id} に対戦終了を通知しました。")
            except Exception as e:
                logging.info(f"[ERROR] end_game の送信失敗: {e}")

        await rdb.delete(f"user:{disconnect_id}")
        await rdb.delete(f"user:{opponent_id}")
        await rdb.delete(f"board:{game_id}")
        
        await rdb.delete(f"turn:{game_id}")
       
        logging.info(f"[CLEANUP] {disconnect_id} と {opponent_id} のデータを削除しました。")

    

    
