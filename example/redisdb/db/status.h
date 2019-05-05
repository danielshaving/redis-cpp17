#pragma once

#include<algorithm>
#include<string>
#include<assert.h>

class Status {
public:
	// Create a success status.
	Status() noexcept : state(nullptr) {

	}

	~Status() {
		if (state != nullptr) {
			free((void*)state);
		}
	}

	Status(const Status& rhs);

	Status& operator=(const Status& rhs);

	Status(Status&& rhs) noexcept : state(rhs.state) { rhs.state = nullptr; }

	Status& operator=(Status&& rhs) noexcept;

	bool operator==(const Status& rhs) const;

	// Return error status of an appropriate type.
	static Status notFound(const std::string_view& msg, const std::string_view& msg2 = std::string_view()) {
		return Status(kNotFound, msg, msg2);
	}

	static Status corruption(const std::string_view& msg, const std::string_view& msg2 = std::string_view()) {
		return Status(kCorruption, msg, msg2);
	}

	static Status OK() { return Status(); }

	static Status notSupported(const std::string_view& msg, const std::string_view& msg2 = std::string_view()) {
		return Status(kNotSupported, msg, msg2);
	}

	static Status invalidArgument(const std::string_view& msg, const std::string_view& msg2 = std::string_view()) {
		return Status(kInvalidArgument, msg, msg2);
	}

	static Status ioError(const std::string_view& msg, const std::string_view& msg2 = std::string_view()) {
		return Status(kIOError, msg, msg2);
	}

	// Returns true iff the status indicates success.
	bool ok() const { return (state == nullptr); }

	// Returns true iff the status indicates a NotFound error.
	bool isNotFound() const { return code() == kNotFound; }

	// Returns true iff the status indicates a Corruption error.
	bool isCorruption() const { return code() == kCorruption; }

	// Returns true iff the status indicates an IOError.
	bool isIOError() const { return code() == kIOError; }

	// Returns true iff the status indicates a NotSupportedError.
	bool isNotSupportedError() const { return code() == kNotSupported; }

	// Returns true iff the status indicates an InvalidArgument.
	bool isInvalidArgument() const { return code() == kInvalidArgument; }

	// Return a string representation of this status suitable for printing.
	// Returns the string "OK" for success.
	std::string toString() const;

private:
	const char* state;

	enum Code {
		kOk = 0,
		kNotFound = 1,
		kCorruption = 2,
		kNotSupported = 3,
		kInvalidArgument = 4,
		kIOError = 5
	};

	Code code() const {
		return (state == nullptr) ? kOk : static_cast<Code>(state[4]);
	}

	Status(Code code, const std::string_view& msg, const std::string_view& msg2);

	static const char* copyState(const char* s);
};

inline Status::Status(const Status& rhs) {
	state = (rhs.state == nullptr) ? nullptr : copyState(rhs.state);
}

inline Status & Status::operator=(const Status& rhs) {
	// The following condition catches both aliasing (when this == &rhs),
	// and the common case where both rhs and *this are ok.
	if (state != rhs.state) {
		free((void*)state);
		state = (rhs.state == nullptr) ? nullptr : copyState(rhs.state);
	}
	return *this;
}

inline Status& Status::operator=(Status&& rhs) noexcept {
	std::swap(state, rhs.state);
	return *this;
}

inline bool Status::operator==(const Status& rhs) const {
	return toString() == rhs.toString();
}

