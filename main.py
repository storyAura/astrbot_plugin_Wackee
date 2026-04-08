import hashlib
import json
import os
import random
import re
import shutil
import uuid
from datetime import datetime

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.star import Context, Star, register
import astrbot.api.message_components as Comp


@register("astrbot_plugin_wackee", "storyAura", "记录群友怪话、统计排行并随机发送", "1.1.2")
class Wackee(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context, config)
        self.config = config
        self.data = {"groups": {}}

    async def initialize(self):
        """插件初始化，加载并兼容迁移持久化数据"""
        self._load_data()
        if self._migrate_data():
            self._save_data()

        total_users = sum(len(g) for g in self.data.get("groups", {}).values())
        total_records = sum(
            len(u.get("records", []))
            for g in self.data.get("groups", {}).values()
            for u in g.values()
        )
        total_occurrences = sum(
            self._get_record_occurrence(rec)
            for g in self.data.get("groups", {}).values()
            for u in g.values()
            for rec in u.get("records", [])
        )
        logger.info(
            f"[Wackee] 怪话记录器已加载，共 {total_users} 位用户，"
            f"{total_records} 条唯一记录，累计出现 {total_occurrences} 次"
        )

    def _get_config_bool(self, key: str, default: bool = False) -> bool:
        if not self.config:
            return default

        try:
            return bool(self.config.get(key, default))
        except Exception:
            return default

    def _is_debug_enabled(self) -> bool:
        return self._get_config_bool("debug_log", False)

    def _debug_log(self, message: str):
        if self._is_debug_enabled():
            logger.info(f"[Wackee][debug] {message}")

    # ==================== 数据持久化 ====================

    def _get_data_dir(self) -> str:
        """获取插件数据存储目录（data/plugin_data/astrbot_plugin_wackee/）"""
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path

        data_dir = os.path.join(
            get_astrbot_data_path(), "plugin_data", "astrbot_plugin_wackee"
        )
        os.makedirs(data_dir, exist_ok=True)
        return data_dir

    def _get_images_dir(self) -> str:
        """获取图片存储目录"""
        images_dir = os.path.join(self._get_data_dir(), "images")
        os.makedirs(images_dir, exist_ok=True)
        return images_dir

    def _get_data_path(self) -> str:
        return os.path.join(self._get_data_dir(), "wackee_data.json")

    def _load_data(self):
        path = self._get_data_path()
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
                if "groups" not in self.data:
                    self.data["groups"] = {}
            except Exception as e:
                logger.error(f"[Wackee] 加载数据失败: {e}")
                self.data = {"groups": {}}
        else:
            self.data = {"groups": {}}

    def _save_data(self):
        try:
            path = self._get_data_path()
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[Wackee] 保存数据失败: {e}")

    def _migrate_data(self) -> bool:
        """兼容旧数据结构，补齐新增字段"""
        changed = False
        groups = self.data.setdefault("groups", {})

        for group_data in groups.values():
            if not isinstance(group_data, dict):
                continue

            for user_data in group_data.values():
                if not isinstance(user_data, dict):
                    continue

                if "records" not in user_data or not isinstance(user_data["records"], list):
                    user_data["records"] = []
                    changed = True

                if "send_count" not in user_data or not isinstance(user_data["send_count"], int):
                    user_data["send_count"] = 0
                    changed = True

                for record in user_data["records"]:
                    if not isinstance(record, dict):
                        continue

                    normalized_occurrence = self._normalize_occurrence_value(
                        record.get("occurrence_count")
                    )
                    if record.get("occurrence_count") != normalized_occurrence:
                        record["occurrence_count"] = normalized_occurrence
                        changed = True

                    if record.get("type") == "image" and not record.get("image_hash"):
                        image_path = record.get("image_path", "")
                        if image_path and os.path.exists(image_path):
                            image_hash = self._hash_file(image_path)
                            if image_hash:
                                record["image_hash"] = image_hash
                                changed = True

        return changed

    # ==================== 图片处理 ====================

    def _normalize_occurrence_value(self, value) -> int:
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            normalized = 1
        return normalized if normalized > 0 else 1

    def _get_record_occurrence(self, record: dict) -> int:
        return self._normalize_occurrence_value(record.get("occurrence_count"))

    def _increment_record_occurrence(self, record: dict) -> int:
        new_count = self._get_record_occurrence(record) + 1
        record["occurrence_count"] = new_count
        return new_count

    def _hash_file(self, file_path: str) -> str | None:
        """计算文件哈希，用于识别重复图片"""
        try:
            digest = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except Exception as e:
            logger.error(f"[Wackee] 计算图片哈希失败: {e}")
            return None

    async def _prepare_image_from_comp(self, image_comp: Comp.Image) -> tuple[str | None, str | None]:
        """从 Image 组件获取本地文件路径和内容哈希"""
        try:
            source_path = await image_comp.convert_to_file_path()
            if not source_path or not os.path.exists(source_path):
                logger.warning("[Wackee] 图片源文件不存在")
                return None, None

            image_hash = self._hash_file(source_path)
            if not image_hash:
                return None, None

            return source_path, image_hash
        except Exception as e:
            logger.error(f"[Wackee] 准备图片失败: {e}")
            return None, None

    def _copy_image_to_data_dir(self, source_path: str) -> str | None:
        """复制图片到插件数据目录"""
        try:
            _, ext = os.path.splitext(source_path)
            if not ext:
                ext = ".jpg"

            filename = f"{uuid.uuid4().hex}{ext}"
            dest_path = os.path.join(self._get_images_dir(), filename)
            shutil.copy2(source_path, dest_path)
            logger.info(f"[Wackee] 图片已保存: {dest_path}")
            return dest_path
        except Exception as e:
            logger.error(f"[Wackee] 保存图片失败: {e}")
            return None

    def _find_existing_text_record(self, user_data: dict, content: str):
        for record in user_data.get("records", []):
            if record.get("type", "text") == "text" and record.get("content", "") == content:
                return record
        return None

    def _find_existing_image_record(self, user_data: dict, content: str, image_hash: str):
        for record in user_data.get("records", []):
            if (
                record.get("type") == "image"
                and record.get("image_hash") == image_hash
                and record.get("content", "") == content
            ):
                return record
        return None

    def _get_user_total_occurrences(self, user_data: dict) -> int:
        return sum(self._get_record_occurrence(record) for record in user_data.get("records", []))

    def _pick_top_rank_record(self, user_data: dict):
        records = user_data.get("records", [])
        if not records:
            return None

        return min(
            records,
            key=lambda record: (
                -self._get_record_occurrence(record),
                record.get("time", ""),
            ),
        )

    def _extract_command_tail(self, message_text: str, command_name: str) -> str:
        text = (message_text or "").strip()
        text = re.sub(r"^[\/／!！]+", "", text).lstrip()

        if text.startswith(command_name):
            text = text[len(command_name) :]

        return text.strip()

    def _clean_target_name(self, target_text: str) -> str:
        text = (target_text or "").strip()
        text = re.sub(r"^[\s@＠]+", "", text)
        return " ".join(text.split()).strip()

    def _normalize_match_name(self, name: str) -> str:
        return "".join((name or "").split()).casefold()

    def _get_member_value(self, member, key: str) -> str:
        if isinstance(member, dict):
            value = member.get(key, "")
        else:
            value = getattr(member, key, "")
        return str(value or "").strip()

    def _collect_member_match_names(self, member) -> list[str]:
        names = []
        seen = set()

        for key in ("card", "nickname", "display_name", "remark", "sender_name"):
            name = self._get_member_value(member, key)
            if not name:
                continue

            normalized_name = self._normalize_match_name(name)
            if not normalized_name or normalized_name in seen:
                continue

            names.append(name)
            seen.add(normalized_name)

        return names

    async def _fetch_aiocqhttp_group_members(self, event: AstrMessageEvent, group_id: str):
        if event.get_platform_name() != "aiocqhttp":
            return []

        bot = getattr(event, "bot", None)
        if not bot or not hasattr(bot, "call_action"):
            return []

        query_group_id = int(group_id) if str(group_id).isdigit() else group_id

        try:
            members = await bot.call_action(
                "get_group_member_list",
                group_id=query_group_id,
            )
            if isinstance(members, list):
                self._debug_log(
                    f"读取 aiocqhttp 原始群成员成功: group_id={group_id}, "
                    f"member_count={len(members)}"
                )
                return members
        except Exception as e:
            self._debug_log(
                f"读取 aiocqhttp 原始群成员失败: group_id={group_id}, error={e}"
            )

        return []

    def _get_group_recorded_name(
        self, group_id: str, user_id: str, fallback_name: str = ""
    ) -> str:
        group_data = self.data.get("groups", {}).get(group_id, {})
        user_data = group_data.get(str(user_id), {})
        sender_name = str(user_data.get("sender_name", "") or "").strip()
        if sender_name:
            return sender_name
        if fallback_name:
            return fallback_name
        return f"用户{user_id}"

    def _match_group_members_by_name(self, members, target_name: str) -> list[dict]:
        normalized_target = self._normalize_match_name(target_name)
        matches = []
        seen_user_ids = set()

        for member in members or []:
            user_id = self._get_member_value(member, "user_id")
            candidate_names = self._collect_member_match_names(member)
            if not user_id or not candidate_names:
                continue

            matched_name = ""
            for candidate_name in candidate_names:
                if self._normalize_match_name(candidate_name) == normalized_target:
                    matched_name = candidate_name
                    break

            if not matched_name:
                continue
            if user_id in seen_user_ids:
                continue

            matches.append({"user_id": user_id, "sender_name": matched_name})
            seen_user_ids.add(user_id)

        return matches

    def _match_recorded_users_by_name(self, group_id: str, target_name: str) -> list[dict]:
        normalized_target = self._normalize_match_name(target_name)
        group_data = self.data.get("groups", {}).get(group_id, {})
        matches = []

        for user_id, user_data in group_data.items():
            sender_name = str(user_data.get("sender_name", "") or "").strip()
            if not sender_name:
                continue
            if self._normalize_match_name(sender_name) != normalized_target:
                continue

            matches.append({"user_id": str(user_id), "sender_name": sender_name})

        return matches

    async def _resolve_target_from_text(
        self, event: AstrMessageEvent, group_id: str, target_text: str
    ) -> dict:
        cleaned_target = self._clean_target_name(target_text)
        if not cleaned_target:
            self._debug_log(
                f"文本目标解析后为空: raw_target={target_text!r}, group_id={group_id}"
            )
            return {"status": "no_match", "target_name": ""}

        self._debug_log(
            f"开始按文本匹配目标: raw_target={target_text!r}, "
            f"cleaned_target={cleaned_target!r}, group_id={group_id}"
        )

        try:
            group = await event.get_group(group_id)
            members = getattr(group, "members", None) or []
            if members:
                matches = self._match_group_members_by_name(members, cleaned_target)
                raw_members = await self._fetch_aiocqhttp_group_members(event, group_id)
                if raw_members:
                    raw_matches = self._match_group_members_by_name(raw_members, cleaned_target)
                    existing_user_ids = {match["user_id"] for match in matches}
                    for match in raw_matches:
                        if match["user_id"] in existing_user_ids:
                            continue
                        matches.append(match)
                        existing_user_ids.add(match["user_id"])
                self._debug_log(
                    f"群成员匹配完成: target={cleaned_target!r}, "
                    f"member_count={len(members)}, match_count={len(matches)}"
                )
                if len(matches) == 1:
                    match = matches[0]
                    self._debug_log(
                        f"文本目标唯一命中群成员: user_id={match['user_id']}, "
                        f"sender_name={match['sender_name']!r}"
                    )
                    return {
                        "status": "ok",
                        "target_id": match["user_id"],
                        "sender_name": match["sender_name"],
                        "target_name": cleaned_target,
                    }
                if len(matches) > 1:
                    self._debug_log(
                        f"文本目标命中多个群成员: target={cleaned_target!r}, "
                        f"user_ids={[match['user_id'] for match in matches]}"
                    )
                    return {"status": "ambiguous", "target_name": cleaned_target}
                self._debug_log(
                    f"群成员中未命中文本目标，继续回退到已记录用户匹配: "
                    f"target={cleaned_target!r}, group_id={group_id}"
                )
            else:
                self._debug_log(
                    f"当前平台未返回可用群成员列表，回退到已记录用户匹配: group_id={group_id}"
                )
        except Exception as e:
            self._debug_log(
                f"获取群成员失败，回退到已记录用户匹配: group_id={group_id}, error={e}"
            )

        matches = self._match_recorded_users_by_name(group_id, cleaned_target)
        self._debug_log(
            f"已记录用户匹配完成: target={cleaned_target!r}, match_count={len(matches)}"
        )

        if len(matches) == 1:
            match = matches[0]
            self._debug_log(
                f"文本目标唯一命中已记录用户: user_id={match['user_id']}, "
                f"sender_name={match['sender_name']!r}"
            )
            return {
                "status": "ok",
                "target_id": match["user_id"],
                "sender_name": match["sender_name"],
                "target_name": cleaned_target,
            }
        if len(matches) > 1:
            self._debug_log(
                f"文本目标命中多个已记录用户: target={cleaned_target!r}, "
                f"user_ids={[match['user_id'] for match in matches]}"
            )
            return {"status": "ambiguous", "target_name": cleaned_target}
        return {"status": "no_match", "target_name": cleaned_target}

    # ==================== 记录怪话 ====================

    @filter.command("记录")
    async def record_wackee(self, event: AstrMessageEvent):
        """引用一条消息并发送「记录」来保存群友的怪话（支持文本和图片）"""
        try:
            msg_obj = event.message_obj
            group_id = str(msg_obj.group_id) if msg_obj.group_id else ""

            if not group_id:
                yield event.plain_result("该指令仅限群聊中使用哦~")
                return

            reply_comp = None
            for comp in msg_obj.message:
                if isinstance(comp, Comp.Reply):
                    reply_comp = comp
                    break

            if not reply_comp:
                yield event.plain_result("请引用/回复一条消息后再发送「记录」哦~")
                return

            content = ""
            sender_id = ""
            sender_name = ""
            image_comps = []

            if hasattr(reply_comp, "message_str") and reply_comp.message_str:
                content = reply_comp.message_str.strip()

            if hasattr(reply_comp, "chain") and reply_comp.chain:
                text_parts = []
                for comp in reply_comp.chain:
                    if isinstance(comp, Comp.Plain):
                        text_parts.append(comp.text)
                    elif isinstance(comp, Comp.Image):
                        image_comps.append(comp)

                if not content:
                    content = "".join(text_parts).strip()

            if hasattr(reply_comp, "sender_id") and reply_comp.sender_id:
                sender_id = str(reply_comp.sender_id)

            if hasattr(reply_comp, "sender_nickname") and reply_comp.sender_nickname:
                sender_name = reply_comp.sender_nickname

            if not sender_id and hasattr(reply_comp, "qq") and reply_comp.qq:
                sender_id = str(reply_comp.qq)

            if not sender_name:
                sender_name = f"用户{sender_id}"

            has_images = len(image_comps) > 0
            has_text = bool(content)
            self._debug_log(
                f"收到记录指令: group_id={group_id}, sender_id={sender_id}, "
                f"sender_name={sender_name!r}, has_text={has_text}, "
                f"has_images={has_images}, content={content!r}"
            )

            if not has_text and not has_images:
                yield event.plain_result("❌ 被引用的消息没有文本或图片内容，无法记录~")
                return

            if not sender_id:
                yield event.plain_result("❌ 无法识别被引用消息的发送者~")
                return

            groups = self.data.setdefault("groups", {})
            group = groups.setdefault(group_id, {})
            user_data = group.setdefault(
                sender_id,
                {
                    "sender_name": sender_name,
                    "records": [],
                    "send_count": 0,
                },
            )
            user_data["sender_name"] = sender_name

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            recorder_name = event.get_sender_name()

            new_records = 0
            updated_records = 0
            occurrence_counts = []

            if has_images:
                for image_comp in image_comps:
                    source_path, image_hash = await self._prepare_image_from_comp(image_comp)
                    if not source_path or not image_hash:
                        continue

                    existing_record = self._find_existing_image_record(
                        user_data, content, image_hash
                    )
                    if existing_record:
                        occurrence_counts.append(
                            self._increment_record_occurrence(existing_record)
                        )
                        updated_records += 1
                        continue

                    image_path = self._copy_image_to_data_dir(source_path)
                    if not image_path:
                        continue

                    user_data["records"].append(
                        {
                            "type": "image",
                            "content": content if content else "",
                            "image_path": image_path,
                            "image_hash": image_hash,
                            "time": now,
                            "recorder": recorder_name,
                            "occurrence_count": 1,
                        }
                    )
                    occurrence_counts.append(1)
                    new_records += 1

            if has_text and not has_images:
                existing_record = self._find_existing_text_record(user_data, content)
                if existing_record:
                    occurrence_counts.append(
                        self._increment_record_occurrence(existing_record)
                    )
                    updated_records += 1
                else:
                    user_data["records"].append(
                        {
                            "type": "text",
                            "content": content,
                            "time": now,
                            "recorder": recorder_name,
                            "occurrence_count": 1,
                        }
                    )
                    occurrence_counts.append(1)
                    new_records += 1

            processed_count = new_records + updated_records
            if processed_count == 0:
                yield event.plain_result("记录失败，请稍后再试~")
                return

            self._save_data()
            self._debug_log(
                f"记录完成: group_id={group_id}, sender_id={sender_id}, "
                f"new_records={new_records}, updated_records={updated_records}, "
                f"processed_count={processed_count}"
            )

            unique_total = len(user_data["records"])
            occurrence_total = self._get_user_total_occurrences(user_data)
            type_desc = "图片怪话" if has_images else "怪话"
            content_preview = f"\n📝「{content}」" if content else ""
            image_info = f"\n🖼️ 含 {len(image_comps)} 张图片" if has_images else ""
            if len(occurrence_counts) == 1:
                occurrence_info = f"\n📈 该条怪话当前已出现 {occurrence_counts[0]} 次"
            else:
                occurrence_info = (
                    "\n📈 本次处理的怪话当前出现次数："
                    + "、".join(str(count) for count in occurrence_counts)
                )

            action_parts = []
            if new_records:
                action_parts.append(f"新增 {new_records} 条")
            if updated_records:
                action_parts.append(f"累计 {updated_records} 条重复记录")
            action_summary = "，".join(action_parts) if action_parts else "已处理"

            yield event.plain_result(
                f"✅ 已记录 {sender_name} 的{type_desc}！"
                f"{content_preview}"
                f"{image_info}"
                f"{occurrence_info}\n"
                f"📌 本次{action_summary}\n"
                f"📊 该用户共有 {unique_total} 条唯一记录，累计出现 {occurrence_total} 次"
            )

        except Exception as e:
            logger.error(f"[Wackee] 记录怪话异常: {e}")
            yield event.plain_result("记录失败，请稍后再试~")

    # ==================== 来句怪话 ====================

    @filter.command("来句怪话")
    async def send_wackee(self, event: AstrMessageEvent):
        """随机发送一条怪话。@某人则发送该用户的怪话，不@则随机抽取本群怪话"""
        try:
            msg_obj = event.message_obj
            group_id = str(msg_obj.group_id) if msg_obj.group_id else ""

            if not group_id:
                yield event.plain_result("该指令仅限群聊中使用哦~")
                return

            target_id = None
            target_name = ""
            for comp in msg_obj.message:
                if isinstance(comp, Comp.At):
                    qq = str(getattr(comp, "qq", "") or "").strip()
                    if not qq or qq == "all":
                        continue
                    target_id = qq
                    target_name = str(getattr(comp, "name", "") or "").strip()
                    break

            self._debug_log(
                f"收到来句怪话: group_id={group_id}, message_str={event.message_str!r}, "
                f"has_real_at={bool(target_id)}, target_id={target_id!r}, "
                f"target_name={target_name!r}"
            )

            if target_id:
                record_info = self._find_targeted_quote(group_id, target_id)
                if record_info:
                    yield self._build_quote_result(
                        event, record_info, target_id=target_id
                    )
                else:
                    display_name = self._get_group_recorded_name(
                        group_id, target_id, target_name
                    )
                    yield event.plain_result(f"没有找到 {display_name} 的怪话记录哦~")
            else:
                raw_target_text = self._extract_command_tail(
                    event.message_str, "来句怪话"
                )
                cleaned_target_name = self._clean_target_name(raw_target_text)
                self._debug_log(
                    f"文本目标提取结果: raw_tail={raw_target_text!r}, "
                    f"cleaned_target={cleaned_target_name!r}"
                )

                if raw_target_text:
                    target_info = await self._resolve_target_from_text(
                        event, group_id, raw_target_text
                    )
                    status = target_info.get("status")
                    if status == "ok":
                        target_id = target_info["target_id"]
                        target_name = target_info["sender_name"]
                        record_info = self._find_targeted_quote(group_id, target_id)
                        if record_info:
                            yield self._build_quote_result(
                                event, record_info, target_id=target_id
                            )
                        else:
                            yield event.plain_result(
                                f"没有找到 {target_name} 的怪话记录哦~"
                            )
                    elif status == "ambiguous":
                        yield event.plain_result(
                            "找到多个同名群友，请使用真实@后再试哦~"
                        )
                    else:
                        yield event.plain_result(
                            "没有找到匹配的群友，请检查昵称或使用真实@哦~"
                        )
                else:
                    record_info = self._find_random_quote(group_id)
                    if record_info:
                        yield self._build_quote_result(event, record_info)
                    else:
                        yield event.plain_result("本群还没有任何怪话记录哦~")

        except Exception as e:
            logger.error(f"[Wackee] 发送怪话异常: {e}")
            yield event.plain_result("发送失败，请稍后再试~")

    @filter.command("怪话帮助")
    async def show_wackee_help(self, event: AstrMessageEvent):
        """显示怪话记录器的指令说明和使用方法"""
        help_text = (
            "Wackee 指令帮助\n"
            "\n"
            "1. 记录\n"
            "功能：保存一条被引用的群消息，支持文本和图片。\n"
            "用法：先引用或回复一条消息，再发送“记录”。\n"
            "说明：重复内容不会新增唯一记录，而是累计出现次数。\n"
            "\n"
            "2. 来句怪话\n"
            "功能：随机发送一条已记录的怪话。\n"
            "用法：发送“来句怪话”。\n"
            "说明：会从当前群的所有记录里随机抽取。\n"
            "\n"
            "3. 来句怪话 @某人\n"
            "功能：随机发送指定用户的一条怪话。\n"
            "用法：发送“来句怪话 @某人”或“来句怪话 某人”。\n"
            "说明：如果 @ 没有成功应用，会尝试按群昵称唯一匹配；若同名多人，请使用真实@。\n"
            "如果开启跨群搜索，本群没有记录时会去其他群查找。\n"
            "\n"
            "4. 怪话排行\n"
            "功能：显示当前群里怪话累计出现次数最多的人。\n"
            "用法：发送“怪话排行”。\n"
            "说明：如果榜首怪话是图片记录，会连图片一起发送。\n"
            "\n"
            "5. 怪话帮助\n"
            "功能：显示本帮助信息。\n"
            "用法：发送“怪话帮助”。\n"
            "\n"
            "注意事项\n"
            "1. 记录、来句怪话、怪话排行仅限群聊中使用。\n"
            "2. 数据保存在 data/plugin_data/astrbot_plugin_wackee/ 目录。"
        )
        yield event.plain_result(help_text)

    @filter.command("怪话排行")
    async def show_wackee_ranking(self, event: AstrMessageEvent):
        """显示当前群聊怪话数量最多的人，并展示其出现最多次的怪话"""
        try:
            msg_obj = event.message_obj
            group_id = str(msg_obj.group_id) if msg_obj.group_id else ""

            if not group_id:
                yield event.plain_result("该指令仅限群聊中使用哦~")
                return

            group_data = self.data.get("groups", {}).get(group_id, {})
            ranking_entries = []

            for user_id, user_data in group_data.items():
                records = user_data.get("records", [])
                if not records:
                    continue

                total_occurrences = self._get_user_total_occurrences(user_data)
                top_record = self._pick_top_rank_record(user_data)
                ranking_entries.append(
                    {
                        "user_id": user_id,
                        "sender_name": user_data.get("sender_name", f"用户{user_id}"),
                        "unique_records": len(records),
                        "total_occurrences": total_occurrences,
                        "top_record": top_record,
                    }
                )

            if not ranking_entries:
                yield event.plain_result("本群还没有任何怪话记录哦~")
                return

            highest_occurrence = max(
                entry["total_occurrences"] for entry in ranking_entries
            )
            winners = [
                entry
                for entry in ranking_entries
                if entry["total_occurrences"] == highest_occurrence
            ]
            winners.sort(key=lambda entry: entry["sender_name"])

            if len(winners) == 1:
                header = "当前群聊怪话第一"
            else:
                header = f"当前群聊怪话并列第一（共 {len(winners)} 人）"

            chain = [Comp.Plain(text=f"{header}\n")]
            for index, entry in enumerate(winners, start=1):
                top_record = entry["top_record"]
                top_content = ""
                top_occurrence = 0
                top_record_type = "text"
                top_image_path = ""

                if top_record:
                    top_content = top_record.get("content", "").strip()
                    top_occurrence = self._get_record_occurrence(top_record)
                    top_record_type = top_record.get("type", "text")
                    top_image_path = top_record.get("image_path", "")

                if top_record_type == "image" and top_content:
                    top_quote = f"图片怪话：{top_content}（出现 {top_occurrence} 次）"
                elif top_record_type == "image":
                    top_quote = f"图片怪话（出现 {top_occurrence} 次）"
                elif top_content:
                    top_quote = f"{top_content}（出现 {top_occurrence} 次）"
                else:
                    top_quote = "暂无可统计的文本怪话"

                section_lines = (
                    f"\n{index}. {entry['sender_name']}\n"
                    f"累计出现：{entry['total_occurrences']} 次\n"
                    f"唯一记录：{entry['unique_records']} 条\n"
                    f"代表怪话：{top_quote}\n"
                )
                chain.append(Comp.Plain(text=section_lines))

                if top_record_type == "image":
                    if top_image_path and os.path.exists(top_image_path):
                        chain.append(Comp.Image.fromFileSystem(top_image_path))
                    else:
                        chain.append(Comp.Plain(text="[图片已丢失]\n"))

                if index < len(winners):
                    chain.append(Comp.Plain(text="--------------------\n"))

            return_result = event.chain_result(chain)
            yield return_result

        except Exception as e:
            logger.error(f"[Wackee] 怪话排行异常: {e}")
            yield event.plain_result("排行生成失败，请稍后再试~")

    # ==================== 辅助方法 ====================

    def _build_quote_result(
        self, event: AstrMessageEvent, record_info: dict, target_id: str | None = None
    ) -> MessageEventResult:
        """根据记录信息构建消息结果（支持图片和文本）"""
        sender_name = record_info["sender_name"]
        record = record_info["record"]
        count = record_info["count"]
        record_type = record.get("type", "text")

        header = f"📢 {sender_name} 曾说过：\n"
        content_text = record.get("content", "")
        footer = f"\n—— {record['time']}（已被发送 {count} 次）"
        lead_text = f"\n{header}" if target_id else header

        if record_type == "image":
            image_path = record.get("image_path", "")
            chain = []
            if target_id:
                chain.append(Comp.At(qq=target_id))
            chain.append(Comp.Plain(text=lead_text))

            if content_text:
                chain.append(Comp.Plain(text=f"「{content_text}」\n"))

            if image_path and os.path.exists(image_path):
                chain.append(Comp.Image.fromFileSystem(image_path))
            else:
                chain.append(Comp.Plain(text="[图片已丢失]"))

            chain.append(Comp.Plain(text=footer))
            return event.chain_result(chain)

        text = f"{lead_text}「{content_text}」{footer}"
        if target_id:
            return event.chain_result([Comp.At(qq=target_id), Comp.Plain(text=text)])
        return event.plain_result(text)

    def _find_targeted_quote(self, group_id: str, target_id: str):
        """查找指定用户的随机怪话，支持跨群搜索"""
        self._debug_log(
            f"开始查找定向怪话: group_id={group_id}, target_id={target_id}"
        )
        result = self._pick_random_from_user(group_id, target_id)
        if result:
            self._debug_log(
                f"在当前群命中定向怪话: group_id={group_id}, target_id={target_id}"
            )
            return result

        cross_group = self._get_config_bool("cross_group_search", False)
        self._debug_log(
            f"当前群未命中定向怪话: group_id={group_id}, "
            f"target_id={target_id}, cross_group_search={cross_group}"
        )

        if not cross_group:
            return None

        for gid in self.data.get("groups", {}):
            if gid == group_id:
                continue
            result = self._pick_random_from_user(gid, target_id)
            if result:
                self._debug_log(
                    f"跨群命中定向怪话: source_group_id={gid}, target_id={target_id}"
                )
                return result

        self._debug_log(
            f"定向怪话查找失败: group_id={group_id}, target_id={target_id}"
        )
        return None

    def _pick_random_from_user(self, group_id: str, target_id: str):
        """从指定群的指定用户中随机选一条怪话，返回包含记录信息的字典"""
        group_data = self.data.get("groups", {}).get(group_id, {})
        user_data = group_data.get(target_id)

        if not user_data or not user_data.get("records"):
            return None

        record = random.choice(user_data["records"])
        sender_name = user_data.get("sender_name", "未知用户")

        user_data["send_count"] = user_data.get("send_count", 0) + 1
        self._save_data()

        return {
            "sender_name": sender_name,
            "record": record,
            "count": user_data["send_count"],
        }

    def _find_random_quote(self, group_id: str):
        """从本群所有用户记录中随机选一条怪话"""
        group_data = self.data.get("groups", {}).get(group_id, {})

        if not group_data:
            return None

        all_entries = []
        for user_id, user_data in group_data.items():
            for record in user_data.get("records", []):
                all_entries.append((user_id, user_data, record))

        if not all_entries:
            return None

        user_id, user_data, record = random.choice(all_entries)
        sender_name = user_data.get("sender_name", "未知用户")

        user_data["send_count"] = user_data.get("send_count", 0) + 1
        self._save_data()

        return {
            "sender_name": sender_name,
            "record": record,
            "count": user_data["send_count"],
        }

    async def terminate(self):
        """插件卸载时保存数据"""
        self._save_data()
        logger.info("[Wackee] 怪话记录器已卸载，数据已保存")
