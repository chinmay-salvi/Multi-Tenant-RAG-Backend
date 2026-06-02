from llama_index.core.callbacks.schema import CBEventType
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import (
    Column,
    String,
    ForeignKey,
    func,
    Boolean,
    DateTime,
    Integer,
    UniqueConstraint,
    PrimaryKeyConstraint,
    ARRAY,
    event,
    Text,
    select,
    TIMESTAMP,
    BigInteger,
)
from sqlalchemy.dialects.postgresql import UUID, ENUM, JSONB
from sqlalchemy.orm import relationship
from enum import Enum


@as_declarative()
class Base:
    __name__: str

    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class MessageRoleEnum(str, Enum):
    user = "user"
    assistant = "assistant"


class MessageStatusEnum(str, Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class MessageQueueStatusEnum(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class MessageSubProcessStatusEnum(str, Enum):
    PENDING = "PENDING"
    FINISHED = "FINISHED"


class DataFeedDataTypeEnum(str, Enum):
    File = "File"
    URL = "URL"
    Text = "Text"


class DataFeedFileTypeEnum(str, Enum):
    PDF = "PDF"
    DOCX = "DOCX"
    JSON = "JSON"
    TXT = "TXT"


# python doesn't allow enums to be extended, so we have to do this
additional_message_subprocess_fields = {
    "CONSTRUCTED_QUERY_ENGINE": "constructed_query_engine",
    "SUB_QUESTIONS": "sub_questions",
}
MessageSubProcessSourceEnum = Enum(
    "MessageSubProcessSourceEnum",
    [(event_type.name, event_type.value) for event_type in CBEventType]
    + list(additional_message_subprocess_fields.items()),
)


def to_pg_enum(enum_class) -> ENUM:
    return ENUM(enum_class, name=enum_class.__name__)


class SysAuthCred(Base):
    authCred = Column(Text, primary_key=True)
    type = Column(String, nullable=False)
    balance = Column(Integer)


class Organization(Base):
    """
    A organization
    """

    orgId = Column(UUID, primary_key=True, index=True)
    orgCreatedAt = Column(DateTime, server_default=func.now(), nullable=False)
    lastTicketSequence = Column(Integer, default=0, nullable=False)

    tags = relationship("Tag", back_populates="organization")
    dataFeeds = relationship("DataFeed", back_populates="organization")
    chatbots = relationship("Chatbot", back_populates="organization")
    organizationUser = relationship("OrganizationUser", back_populates="organization")


class User(Base):
    """
    A user within an organization
    """

    userId = Column(UUID, primary_key=True, index=True)
    userCreateAt = Column(DateTime, server_default=func.now(), nullable=False)

    organizationUser = relationship("OrganizationUser", back_populates="user")


class OrganizationUser(Base):
    userId = Column(UUID(as_uuid=True), ForeignKey("user.userId"), index=True)
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)
    userRole = Column(String, nullable=False)

    # Define the composite primary key
    __table_args__ = (PrimaryKeyConstraint(userId, orgId),)

    organization = relationship("Organization", back_populates="organizationUser")
    user = relationship("User", back_populates="organizationUser")


class Tag(Base):
    """
    A datafeed tag
    """

    tagId = Column(UUID, primary_key=True, index=True, default=func.uuid_generate_v4())
    tagName = Column(String, nullable=False)
    tagCreatedAt = Column(DateTime, server_default=func.now(), nullable=False)
    tagUpdatedAt = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)

    organization = relationship("Organization", back_populates="tags")
    dataFeedTags = relationship("DataFeedTag", back_populates="tag")

    # Add a unique constraint to ensure tag and orgId pair is unique
    __table_args__ = (UniqueConstraint("tagName", "orgId", name="unique_tag_orgId"),)


class DeletedDataFeed(Base):
    """
    A deleted datafeed
    """

    deletedDataFeedId = Column(
        UUID, primary_key=True, index=True, default=func.uuid_generate_v4()
    )
    dataFeedId = Column(UUID)
    dataType = Column(to_pg_enum(DataFeedDataTypeEnum), nullable=False)
    fileType = Column(to_pg_enum(DataFeedFileTypeEnum))
    dataFeedName = Column(String, nullable=False)
    title = Column(String)
    mainURL = Column(String)
    dataFeedURL = Column(String)
    createdDatetime = Column(DateTime, nullable=False)
    updatedDatetime = Column(DateTime, nullable=False)
    deletedDatetime = Column(DateTime, server_default=func.now(), nullable=False)
    tokenCount = Column(Integer, default=-1, nullable=False)
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)


class DataFeed(Base):
    """
    A datafeed
    """

    dataFeedId = Column(
        UUID, primary_key=True, index=True, default=func.uuid_generate_v4()
    )
    dataFeedName = Column(String, nullable=False)
    title = Column(String)
    dataType = Column(to_pg_enum(DataFeedDataTypeEnum), nullable=False)
    fileType = Column(to_pg_enum(DataFeedFileTypeEnum))
    mainURL = Column(String)
    dataFeedURL = Column(String)
    createdDatetime = Column(DateTime, server_default=func.now(), nullable=False)
    updatedDatetime = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    tokenCount = Column(Integer, default=-1, nullable=False)
    activeStatus = Column(Integer, default=0, nullable=False)
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)

    organization = relationship("Organization", back_populates="dataFeeds")
    dataFeedTags = relationship("DataFeedTag", back_populates="dataFeed")
    chatbotDataFeeds = relationship("ChatbotDataFeeds", back_populates="datafeed")



class URLScrapingMessageQueue(Base):
    groupId = Column(UUID, primary_key=True, index=True, nullable=False)
    userId = Column(UUID, index=True, nullable=False)
    messageStatus = Column(to_pg_enum(MessageQueueStatusEnum))
    mainURL = Column(String)
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"))
    createdAt = Column(TIMESTAMP, index=True, server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "groupId",
            "userId",
            "orgId",
            name="unique_groupId_userId_orgId",
        ),
    )


class DataFeedTag(Base):
    """
    Tag associated with datafeed within organization
    """

    dataFeedId = Column(
        UUID(as_uuid=True), ForeignKey("datafeed.dataFeedId"), index=True
    )
    tagId = Column(UUID(as_uuid=True), ForeignKey("tag.tagId"), index=True)
    createTime = Column(DateTime, server_default=func.now(), nullable=False)
    updateTime = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Define the composite primary key
    __table_args__ = (PrimaryKeyConstraint(dataFeedId, tagId),)

    dataFeed = relationship("DataFeed", back_populates="dataFeedTags")
    tag = relationship("Tag", back_populates="dataFeedTags")


class TempUrlDataFeed(Base):
    urlId = Column(UUID, primary_key=True, index=True, default=func.uuid_generate_v4())
    groupId = Column(UUID, index=True)
    url = Column(String)
    pageTitle = Column(String)
    mainURL = Column(String)
    content = Column(String)
    createdDatetime = Column(DateTime, server_default=func.now(), nullable=False)
    updatedDatetime = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    tokenCount = Column(Integer, default=-1, nullable=False)
    userId = Column(UUID(as_uuid=True), ForeignKey("user.userId"), index=True)


class AgentType(Base):
    agentTypeId = Column(
        UUID, primary_key=True, index=True, default=func.uuid_generate_v4()
    )
    agentTypeName = Column(String, nullable=False)
    agentTypeCreationDate = Column(DateTime, server_default=func.now(), nullable=False)

    chatbots = relationship("Chatbot", back_populates="agentType")


class Chatbot(Base):
    """
    A chatbot
    """

    chatbotId = Column(
        UUID, primary_key=True, index=True, default=func.uuid_generate_v4()
    )
    chatbotName = Column(String, nullable=False)
    chatbotCreationDate = Column(DateTime, server_default=func.now(), nullable=False)
    chatbotUpdationDate = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    chatbotLink = Column(String)
    chatbotBannerMessage = Column(String)
    chatWithUsTitle = Column(String)
    chatWithUsDescription = Column(String)
    raiseTicketTitle = Column(String)
    raiseTicketDescription = Column(String)
    chatbotUserMessageColor = Column(String)
    hallucinationFixer = Column(String)
    chatbotFor = Column(String)
    companyDo = Column(String)
    companyLogo = Column(String)
    companyLogoBase64 = Column(String)
    widgetLogo = Column(String)
    widgetLogoBase64 = Column(String)
    businessContactDetails = Column(String)
    chatbotTicketingVisibility = Column(Boolean, nullable=False, default=False)
    isPublic = Column(Boolean, nullable=False, default=False)
    isLeadFormEnabled = Column(Boolean, nullable=False, default=False)
    activeStatus = Column(Boolean, nullable=False, default=True)
    chatbotIntroMessages = Column(ARRAY(String), default=[])
    chatbotExampleUserQuestions = Column(ARRAY(String), default=[])
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)
    agentTypeId = Column(UUID(as_uuid=True), ForeignKey("agenttype.agentTypeId"))
    latestLeadFormId = Column(UUID(as_uuid=True), ForeignKey("leadform.leadFormId"))

    organization = relationship("Organization", back_populates="chatbots")
    conversations = relationship("Conversation", back_populates="chatbot")
    # chatbotIntroMessages = relationship("ChatbotIntroMessage", back_populates="chatbot")
    # chatbotExampleUserQuestions = relationship(
    #     "ChatbotExampleUserQuestions", back_populates="chatbot"
    # )
    chatbotSupportTicketingCategories = relationship(
        "ChatbotSupportTicketingCategories", back_populates="chatbot"
    )
    tickets = relationship("Tickets", back_populates="chatbot")
    chatbotDataFeeds = relationship("ChatbotDataFeeds", back_populates="chatbot")
    agentType = relationship("AgentType", back_populates="chatbots")
    leadForm = relationship(
        "LeadForm", back_populates="chatbot", foreign_keys="LeadForm.chatbotId"
    )
    latestLeadForm = relationship(
        "LeadForm", foreign_keys=[latestLeadFormId], uselist=False
    )
    leads = relationship("Lead", back_populates="chatbot")


class ChatbotDataFeeds(Base):
    chatbotId = Column(UUID(as_uuid=True), ForeignKey("chatbot.chatbotId"), index=True)
    dataFeedId = Column(
        UUID(as_uuid=True), ForeignKey("datafeed.dataFeedId"), index=True
    )

    # Define the composite primary key
    __table_args__ = (PrimaryKeyConstraint(chatbotId, dataFeedId),)

    chatbot = relationship("Chatbot", back_populates="chatbotDataFeeds")
    datafeed = relationship("DataFeed", back_populates="chatbotDataFeeds")


class ChatbotSupportTicketingCategories(Base):
    """
    Support ticket categories details for chatbot
    """

    createTime = Column(DateTime, server_default=func.now(), nullable=False)
    updateTime = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    supportTicketingCategoryTitle = Column(String)
    supportTicketingCategoryDescription = Column(String)
    supportTicketingCategorySequence = Column(
        Integer, nullable=False, autoincrement=True
    )
    chatbotId = Column(UUID(as_uuid=True), ForeignKey("chatbot.chatbotId"), index=True)

    # Define the composite primary key
    __table_args__ = (
        PrimaryKeyConstraint(supportTicketingCategorySequence, chatbotId),
    )

    chatbot = relationship(
        "Chatbot", back_populates="chatbotSupportTicketingCategories"
    )


class Tickets(Base):
    """
    Ticket raised by the customer
    """

    ticketId = Column(Integer, index=True, nullable=False)
    category = Column(String, nullable=False)  # enum later
    status = Column(String, nullable=False)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False)
    issue = Column(String, nullable=False)
    priority = Column(
        String, nullable=False, default="Medium"
    )  # enum later - default medium
    createdTime = Column(DateTime, server_default=func.now(), nullable=False)
    updateTime = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    closedTime = Column(DateTime)
    fileType = Column(String, nullable=False)  # enum later
    fileURL = Column(String, default=None)

    chatbotId = Column(UUID(as_uuid=True), ForeignKey("chatbot.chatbotId"), index=True)
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)

    # Define the composite primary key
    __table_args__ = (PrimaryKeyConstraint(ticketId, orgId),)

    chatbot = relationship("Chatbot", back_populates="tickets")


class Conversation(Base):
    """
    A conversation with messages
    """

    conversationId = Column(
        UUID, primary_key=True, index=True, default=func.uuid_generate_v4()
    )
    createdAt = Column(DateTime, server_default=func.now(), nullable=False)
    updatedAt = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    totalConversationMessage = Column(Integer, default=0)
    botMessage = Column(String)
    userMessage = Column(String)
    rating = Column(Integer, default=None)
    managedConversationId = Column(BigInteger, default=None, index=True)
    chatbotId = Column(UUID(as_uuid=True), ForeignKey("chatbot.chatbotId"), index=True)
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)

    leads = relationship("Lead", back_populates="conversation")
    messages = relationship("Message", back_populates="conversation")
    chatbot = relationship("Chatbot", back_populates="conversations")


class Message(Base):
    """
    A message in a conversation
    """

    messageId = Column(
        UUID, primary_key=True, index=True, default=func.uuid_generate_v4()
    )
    conversationId = Column(
        UUID(as_uuid=True), ForeignKey("conversation.conversationId"), index=True
    )
    content = Column(String)
    role = Column(to_pg_enum(MessageRoleEnum))
    status = Column(to_pg_enum(MessageStatusEnum), default=MessageStatusEnum.PENDING)
    createdAt = Column(DateTime, server_default=func.now(), nullable=False)
    updatedAt = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)

    conversation = relationship("Conversation", back_populates="messages")
    subProcesses = relationship("MessageSubProcess", back_populates="message")


class MessageSubProcess(Base):
    """
    A record of a sub-process that occurred as part of the generation of a message from an AI assistant
    """

    messageSubProcessId = Column(
        UUID, primary_key=True, index=True, default=func.uuid_generate_v4()
    )
    messageId = Column(UUID(as_uuid=True), ForeignKey("message.messageId"), index=True)
    createdAt = Column(DateTime, nullable=False)
    source = Column(to_pg_enum(MessageSubProcessSourceEnum))
    status = Column(
        to_pg_enum(MessageSubProcessStatusEnum),
        default=MessageSubProcessStatusEnum.FINISHED,
        nullable=False,
    )
    metadataMap = Column(JSONB, nullable=True)

    message = relationship("Message", back_populates="subProcesses")


class LeadForm(Base):
    leadFormId = Column(
        UUID, primary_key=True, index=True, default=func.uuid_generate_v4()
    )
    createdAt = Column(DateTime, server_default=func.now(), nullable=False)
    title = Column(String, nullable=False)
    keys = Column(ARRAY(String), default=[])
    labels = Column(ARRAY(String), default=[])
    inputTypes = Column(ARRAY(String), default=[])
    frequencyHours = Column(Integer)
    maxShowLimit = Column(Integer)

    chatbotId = Column(UUID(as_uuid=True), ForeignKey("chatbot.chatbotId"), index=True)
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)

    chatbot = relationship(
        "Chatbot",
        back_populates="leadForm",
        foreign_keys=[chatbotId],
    )
    leads = relationship("Lead", back_populates="leadForm")


class Lead(Base):
    leadId = Column(UUID, primary_key=True, index=True, default=func.uuid_generate_v4())
    createdAt = Column(DateTime, server_default=func.now(), nullable=False)
    data = Column(JSONB, nullable=False)

    leadFormId = Column(
        UUID(as_uuid=True),
        ForeignKey("leadform.leadFormId"),
        index=True,
    )
    conversationId = Column(
        UUID(as_uuid=True), ForeignKey("conversation.conversationId"), index=True
    )
    chatbotId = Column(UUID(as_uuid=True), ForeignKey("chatbot.chatbotId"), index=True)
    orgId = Column(UUID(as_uuid=True), ForeignKey("organization.orgId"), index=True)

    leadForm = relationship("LeadForm", back_populates="leads")
    conversation = relationship("Conversation", back_populates="leads")
    chatbot = relationship("Chatbot", back_populates="leads")
